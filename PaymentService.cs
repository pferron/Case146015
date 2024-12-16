using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Common.Logging;
using ECMBLL;
using SWBC.ECM.BLL;
using SWBC.ECM.BO.Enumerations;
using SWBC.ECM.DAL;
using SWBC.ECM.DAL.Repositories;
using SWBC.Payments.Business.Common.Exceptions;
using SWBC.Payments.Business.Common.Extensions;
using SWBC.Payments.Business.Common.Interfaces;
using SWBC.Payments.Business.Common.Objects;
using SWBC.Payments.Business.Extensions;
using SWBC.Payments.Facade;
using SWBC.Payments.Message.Commands;
using SWBC.Payments.Message.Views;
using SWBC.ProtectPay.Connector.Dtos;
using SWBC.Utilities.Domain.Validation;
using SWBC.Utilities.Portal.Client.Views;
using PaymentConstants = SWBC.Payments.Business.Common.Constants;
using POCORepo = SWBC.ECM.BO.POCO;

namespace SWBC.Payments.Business
{
    public class PaymentService : BaseService, IPaymentService
    {
        private const string UnknownStatusObtained = "UNKNOWN";
        private const string ReversalLogText = "Reversal";
        private const string RefundTotal = "Refund Total";
        private const char CapCharSymbol = '^';
        private const string Acculynk = "acculynk";
        private const string CC = "CC";
        private const string InvalidRequest = "INVALID";
        private static string SettlementComments = "Transaction Edited: {0} - What Changed: {1}";
        private static string ReversalCompleteErrorMsg = "Error: Recover amount cannot be more than the original transaction amount: {0}";
        private readonly IEcmUserService _ecmUserService;
        private readonly IPortalService _portalService;
        private static ILog log = LogManager.GetLogger<PaymentService>();
        private readonly ITransactionManager _transactionManager;
        private readonly IPortalAccountEntityMappingService _mappingService;
        private readonly ITransactionProcessingDetailService _transactionProcessingDetailService;
        private readonly IUserSettingsService _userSettingsService;
        private readonly IPortalAccountEntityMappingCache _mappingCache;
        private readonly IValidationService _validationService;
        private readonly IDateUtilities _dateUtility;
        private readonly IAddressService _addressService;

        public PaymentService(ICurrentPaymentsUser currentPaymentsUser, IEcmUserService ecmUserService,
            IPortalService portalService, ITransactionManager transactionManager,
            IPortalAccountEntityMappingService mappingService, ITransactionProcessingDetailService transactionProcessingDetailService,
            IPaymentsAccountManager paymentsAccountManager, IUserSettingsService userSettingsService, IPortalAccountEntityMappingCache mappingCache,
            IValidationService validationService, IDateUtilities dateUtility, IAddressService addressService)
            : base(currentPaymentsUser, paymentsAccountManager)
        {
            _mappingService = mappingService ?? throw new ArgumentNullException(nameof(mappingService));
            CurrentPaymentsUser.PortalAccountEntityMappingService = mappingService;
            _ecmUserService = ecmUserService ?? throw new ArgumentNullException(nameof(ecmUserService));
            _portalService = portalService ?? throw new ArgumentNullException(nameof(portalService));
            _transactionManager = transactionManager ?? throw new ArgumentNullException(nameof(transactionManager));
            _transactionProcessingDetailService = transactionProcessingDetailService ?? throw new ArgumentNullException(nameof(transactionProcessingDetailService));
            _userSettingsService = userSettingsService ?? throw new ArgumentNullException(nameof(userSettingsService));
            _mappingCache = mappingCache ?? throw new ArgumentNullException(nameof(mappingCache));
            _validationService = validationService ?? throw new ArgumentNullException(nameof(validationService));
            _dateUtility = dateUtility ?? throw new ArgumentNullException(nameof(dateUtility));
            _addressService = addressService ?? throw new ArgumentNullException(nameof(addressService));
        }

        public async Task<string> ReversalRequestSubmission(ReversalRequestCommand command)
        {
            CurrentPaymentsUser.VerifyAuthorizationFor(Common.Enums.FunctionalSpecs.REQUEST_ACH_REVERSAL);
            var achTransaction = ACHTransactionManager.GetItem(command.TrackingNumber);

            if (achTransaction == null || achTransaction.Status.ToUpperInvariant() == PaymentConstants.ReversalRequest.ToUpperInvariant())
            {
                return InvalidRequest;
            }

            var portalUser = await GetPortalUser();
            var paymentsAccount = PaymentsAccountManager.GetPaymentsAccount(int.Parse(achTransaction.EntityID));
            var portalAccountId = _mappingService.GetPortalAccountForEntityId(int.Parse(achTransaction.EntityID));
            var alertDetailsDto = new AlertDetailsDto()
            {
                AchCustomerNumber = paymentsAccount.AchCustomerNumber,
                AS400BatchId = achTransaction.AS400BatchID,
                IndividualName = achTransaction.IndividualName,
                AccountTypeDisplay = achTransaction.AccountTypeDisplay,
                Amount = achTransaction.Amount.ToString("C"),
                EffectiveDate = achTransaction.EffectiveDate.ToString("MM/dd/yyyy"),
                ReversalReason = ReversalLogText,
                PortalAccountId = portalAccountId,
                TrackingNumber = command.TrackingNumber,
                AccountName = paymentsAccount.InstName
            };
            var portalAlertService = new PortalAlertService();
            portalAlertService.PostAlertToPortal(Facade.Enums.PaymentsAlertTypes.PendingAchReversal, alertDetailsDto);
            ACHTransactionLogManager.Insert(achTransaction.StrKey, portalUser.FullName, DateTime.Now, ReversalLogText, command.Reason, string.Empty);
            ACHTransactionManager.UpdateStatusInTblTransHistory(PaymentConstants.ReversalRequest, achTransaction.TrackingNumber, PaymentConstants.System);
            return string.Empty;
        }

        public async Task<string> ReversalApprovalSubmission(ReversalApprovalCommand command)
        {
            int entityId = 0;

            CurrentPaymentsUser.VerifyAuthorizationFor(Common.Enums.FunctionalSpecs.APPROVE_ACH_REVERSAL);

            var portalUser = await GetPortalUser();
            var achTransaction = ACHTransactionManager.GetItem(command.TrackingNumber);
            var ecmAccount = ECMAccountManager.GetItem(achTransaction.EntityID);
            int.TryParse(ecmAccount.EntityID, out entityId);

            if (achTransaction != null && achTransaction.Status.ToUpperInvariant() == PaymentConstants.ReversalComplete.ToUpperInvariant())
            {
                return InvalidRequest;
            }

            var (clientTransactions, fundsRecoveryTransComplete, processFundsResult) = ProcessFundsRecovery(command, entityId, achTransaction, ecmAccount);

            var (reversalComplete, processReversalResult, innerEx) = ProcessReversalStatus(achTransaction, portalUser, command, clientTransactions, fundsRecoveryTransComplete);

            string finalResult = DetermineFinalResult(processFundsResult, processReversalResult);

            //--- Clear alert:  Funds Recovery Transactions has been submitted (even if status change failed)
            //---               OR there was no Funds Recovery required, and status was updated successfully
            if (fundsRecoveryTransComplete || reversalComplete)
            {
                var alertDetailsDto = new AlertDetailsDto()
                {
                    TrackingNumber = command.TrackingNumber,
                    PortalAccountId = CurrentPaymentsUser.SelectedAccountId
                };
                var portalAlertService = new PortalAlertService();
                portalAlertService.DeleteAlertFromPortal(Facade.Enums.PaymentsAlertTypes.PendingAchReversal, alertDetailsDto, CurrentPaymentsUser.UserId);
            }

            //--- Finally, after DeleteAlert has been attempted, log error (if any) and throw UserReadableException
            if (!string.IsNullOrEmpty(finalResult))
            {
                log.Error(finalResult + $" StrKey: {achTransaction.StrKey}", innerEx);
                throw new UserReadableException(finalResult);
            }

            // If no errors found, then verify if transaction is ready to post to C2C
            CompleteReversalC2C(command, portalUser, achTransaction, fundsRecoveryTransComplete, reversalComplete);

            //--- If no errors encountered, return empty string...
            return finalResult;
        }

        private (List<POCORepo.ClientTransHistory> clientTransactions, bool fundsRecoveryTransComplete, string errorResult)
        ProcessFundsRecovery( ReversalApprovalCommand command,
            int entityId,
            ACHTransaction achTransaction,
            ECMAccount ecmAccount)
        {
            var clientTransactions = new List<POCORepo.ClientTransHistory>();
            bool fundsRecoveryTransComplete = false;
            string result = string.Empty;

            if (command.IsFundsRecovery)
            {
                if (Convert.ToDecimal(command.Amount) <= Convert.ToDecimal(achTransaction.Amount))
                {
                    try
                    {
                        var clientDebitTrans = ClientTransactionSubmissionAgent.SubmitNewSingleClientTransaction(
                            entityId,
                            TransactionType.ClientReversal,
                            command.Amount,
                            ecmAccount.InstABANumber,
                            ecmAccount.CCDAccountType,
                            ECMDataSecurity.ConfigKeyDESDecrypt(ecmAccount.CCDAccountNumber),
                            PaymentConstants.ACH_SETTLEMENT_BATCH_TYPE,
                            string.Concat(PaymentConstants.REVERSAL_TRANSACTION_PREFIX, " ", ecmAccount.InstNameForCCD),
                            PaymentConstants.ORIG_APPID,
                            PaymentConstants.AS400DEBIT,
                            ecmAccount.ACHCustomerNumber,
                            ecmAccount.SWBCBankNumber.ToString(),
                            DateTime.Now,
                            PaymentSource.OnlineECM);

                        clientTransactions.Add(clientDebitTrans);

                        bool.TryParse(ConfigurationManager.AppSettings.Get("EnableConsumerCreditCaptured"), out bool consumerCreditCapturedEnable);

                        if (consumerCreditCapturedEnable)
                        {
                            var consumerCreditTrans = ClientTransactionSubmissionAgent.SubmitNewSingleClientTransaction(
                                entityId,
                                TransactionType.ClientReversal,
                                command.Amount,
                                achTransaction.ABA,
                                ecmAccount.CCDAccountType,
                                achTransaction.ACHAccountNumber,
                                PaymentConstants.ACH_SETTLEMENT_BATCH_TYPE,
                                string.Concat(PaymentConstants.REVERSAL_TRANSACTION_PREFIX, " ", ecmAccount.InstNameForCCD),
                                PaymentConstants.ORIG_APPID,
                                PaymentConstants.AS400CREDIT,
                                string.Empty,
                                string.Empty,
                                DateTime.Now,
                                PaymentSource.OnlineECM);

                            clientTransactions.Add(consumerCreditTrans);
                        }

                        fundsRecoveryTransComplete = true;
                    }
                    catch (ReturnValueException ex)
                    {
                        result = GetAchError(ex.ReturnValue);
                        result = string.Concat("Error: Unable to submit Reversal (Funds Recovery) Transaction:", result);
                        throw new UserReadableException(result);
                    }
                }
                else
                {
                    throw new UserReadableException(string.Format(ReversalCompleteErrorMsg, string.Format("{0:C}", command.Amount)));
                }
            }
            return (clientTransactions, fundsRecoveryTransComplete, result);
        }

        private (bool reversalComplete, string result, Exception innerEx) ProcessReversalStatus(ACHTransaction achTransaction, UserView portalUser, ReversalApprovalCommand command, List<POCORepo.ClientTransHistory> clientTransactions, bool fundsRecoveryTransComplete)
        {
            string result = string.Empty;
            Exception innerEx = null;
            bool reversalComplete = false;
            bool isFirstTransaction = true;

            try
            {
                if (ACHTransactionManager.UpdateStatusInTblTransHistory(PaymentConstants.ReversalComplete, achTransaction.TrackingNumber, portalUser.FullName))
                {
                    reversalComplete = true;
                    if (command.IsFundsRecovery)
                    {
                        foreach (var clientTrans in clientTransactions)
                        {
                            ACHClientTransaction newAchTrans = ACHClientTransactionManager.GetItem(clientTrans.TrackingNumber);

                            string submittedMessage = $"Submitted for a Reversal of tracking number: {achTransaction.TrackingNumber}";
                            if (ACHClientTransactionLogManager.Insert(newAchTrans.StrKey, portalUser.FullName, DateTime.Now, PaymentConstants.SUBMITTED_ACTION, submittedMessage))
                            {
                                string reversalPrefix = isFirstTransaction ? "Client/Consumer Debit" : "Reversal and Member Credit";
                                string completedMessage = $"{reversalPrefix} Completed, Tracking Number: {clientTrans.TrackingNumber}";

                                if (ACHTransactionLogManager.Insert(achTransaction.StrKey, portalUser.FullName, DateTime.Now, "Reversal Completed", completedMessage, "0"))
                                {
                                    //This condition verifies a negative transaction amount to identify a Client / Consumer Debit transaction type, after which we can post a note.
                                    if (clientTrans.Amount < 0)
                                    {
                                        string recoveredMessage = $"Reversal Funds recovered: Reason: {command.Reason}";
                                        ACHTransactionLogManager.Insert(achTransaction.StrKey, portalUser.FullName, DateTime.Now, "Note", recoveredMessage, "0");
                                    }
                                }
                                else
                                {
                                    result = "Unable to insert ACH log comment for ACH Reversal, but Reversal Transaction has been submitted.  Please contact webmaster@swbc.com.";
                                }
                            }
                            else
                            {
                                result = "Unable to insert CCD log comment for ACH Reversal, but Reversal Transaction has been submitted.  Please contact webmaster@swbc.com.";
                            }

                            isFirstTransaction = false;
                        }
                    }
                    else
                    {
                        ACHTransactionLogManager.Insert(achTransaction.StrKey, portalUser.FullName, DateTime.Now, "Note", string.Concat("Reversal funds not recovered: Reason: ", command.Reason), "0");
                    }
                }
                else
                {
                    result = fundsRecoveryTransComplete
                            ? "Unable to update Status of ACH Reversal, but the Reversal (Funds Recovery) Transaction has been submitted.  Please contact webmaster@swbc.com."
                            : "Unable to complete ACH Reversal.  Please contact webmaster@swbc.com.";
                }
            }
            catch (Exception ex)
            {
                result = "Unable to update Status or log of ACH Reversal due to an unexpected error, but the Reversal Transaction has been submitted.  Please contact webmaster@swbc.com.";
                innerEx = ex;
            }

            return (reversalComplete, result, innerEx);
        }

        private string DetermineFinalResult(string processFundsResult, string processReversalResult)
        {
            string finalResult = string.Empty;
            if (!string.IsNullOrEmpty(processFundsResult))
            {
                finalResult = processFundsResult;
            }
            else if (!string.IsNullOrEmpty(processReversalResult))
            {
                finalResult = processReversalResult;
            }
            return finalResult;
        }


        private void CompleteReversalC2C(ReversalApprovalCommand command, UserView portalUser, ACHTransaction achTransaction, bool fundsRecoveryTransComplete, bool reversalComplete)
        {
            var strKeyInt = 0;
            int.TryParse(achTransaction.StrKey, out strKeyInt);
            try
            {
                var entityIdInt = 0;
                int.TryParse(achTransaction.EntityID, out entityIdInt);
                var pai = PortalAccountMapping.GetInfoForEntityId(entityIdInt) ?? throw new Exception($"Portal Account Info null for entity id: {achTransaction.EntityID}");
                if (!string.IsNullOrWhiteSpace(pai.FinancialInstId))
                {
                    if (fundsRecoveryTransComplete && reversalComplete)
                    {
                        new C2CService().PostTransactionToConnect2CoreGateway(C2C.DTOs.CUFXBase.TransactionTypeToC2C.Reversal, command.TrackingNumber.ToString(), PaymentConstants.ReversalComplete, portalUser.FullName, CurrentPaymentsUser.SelectedAccountId);
                    }
                    else if (!command.IsFundsRecovery && reversalComplete && strKeyInt != 0)
                    {
                        var splitPayments = SingleACHTransactionAgent.GetSplitPaymentDetails(strKeyInt);
                        var partyId = splitPayments.FirstOrDefault(x => !string.IsNullOrWhiteSpace(x.C2CMessage.FirstOrDefault(y => !string.IsNullOrWhiteSpace(y.PartyMemberId))?.PartyMemberId))?.C2CMessage?.FirstOrDefault(x => !string.IsNullOrWhiteSpace(x.PartyMemberId))?.PartyMemberId ?? string.Empty;
                        var c2cTransaction = new C2CService().PrepareC2CTransaction(pai, portalUser.FullName, partyId, strKeyInt, achTransaction.PaymentSourceId);
                        foreach (var sp in splitPayments)
                        {
                            var decryptedAccountNumber = ECMDataSecurity.ECMDecrypt(sp.ApplyToAccountNum);
                            sp.ApplyToAccountNum = decryptedAccountNumber;
                            sp.PayToAccountNumber = decryptedAccountNumber;
                            var c2cBO = new ECM.BO.Classes.C2CMessageBO(sp, c2cTransaction, false, DateTime.Now, TransactionTypeC2C.Reversal, portalUser.FullName, false, PostStatusC2C.NotApplicable);
                            var c2cMessageAmbiguous = C2CTransactionAgent.CreateC2CMessage(c2cBO);

                            if (pai.PostReversals)
                            {
                                C2CTransactionAgent.SaveC2CMessage(c2cMessageAmbiguous);
                                C2CTransactionAgent.CreateC2CHistoryRecord(sp, c2cMessageAmbiguous, "No Gateway", "No Gateway", PostStatusC2C.NotApplicable, 0);
                            }

                            if (pai.PostReversalNotesToMembers)
                            {
                                c2cMessageAmbiguous.PostType = (int)TransactionTypeC2C.ReversalNote;
                                c2cMessageAmbiguous.PostStatus = (int)PostStatusC2C.NotApplicable;
                                C2CTransactionAgent.SaveC2CMessage(c2cMessageAmbiguous);
                                C2CTransactionAgent.CreateC2CHistoryRecord(sp, c2cMessageAmbiguous, "No Gateway", "No Gateway", PostStatusC2C.NotApplicable, 0);
                            }

                            if (pai.PostReversalNotesToAccounts)
                            {
                                c2cMessageAmbiguous.PostType = (int)TransactionTypeC2C.ReversalAccountNote;
                                c2cMessageAmbiguous.PostStatus = (int)PostStatusC2C.NotApplicable;
                                C2CTransactionAgent.SaveC2CMessage(c2cMessageAmbiguous);
                                C2CTransactionAgent.CreateC2CHistoryRecord(sp, c2cMessageAmbiguous, "No Gateway", "No Gateway", PostStatusC2C.NotApplicable, 0);
                            }
                        }
                    }
                    else throw new Exception("ACH reversal for C2C contains insufficient information.");
                }
            }
            catch (Exception ex)
            {
                log.Error($"C2C - ACH approve reversal cannot be complete for StrKey: {achTransaction.StrKey}", ex);
                C2CTransactionAgent.CreateTransLogEntry("C2C - Reversal messages could not be complete.", strKeyInt, portalUser.FullName, "Approve ACH reversal");
            }
        }

        public async Task<string> RefundSubmission(int strKey)
        {
            CurrentPaymentsUser.VerifyAuthorizationFor(Common.Enums.FunctionalSpecs.REFUND_CC_PAYMENT);
            var portalUser = await GetPortalUser();
            ACHTransactionWithCC achTransactionWithCC = CreditCardTransactionManager.GetItemByStrKey(strKey);
            var ecmAccount = ECMAccountManager.GetItem(achTransactionWithCC.EntityID);

            if ((achTransactionWithCC != null && achTransactionWithCC.Status.ToUpperInvariant() == PaymentConstants.REFUNDED.ToUpperInvariant())
                || !_validationService.PerformReversalOrVoidRefund(achTransactionWithCC.Status, achTransactionWithCC.EffectiveDate, Constants.CC_TRANSACTION, strKey, ecmAccount.SWBCHandlesCC, ecmAccount.GenPaymentRunTime))
            {
                return InvalidRequest;
            }

            try
            {
                CreditCardTransResponse response = achTransactionWithCC.TransTypeValue == TransactionType.DebitPinAcculynk ?
                    CreditCardRefundManager.RefundAcculynkPinDebitCardTransaction(achTransactionWithCC, RefundTotal, portalUser.FullName) :
                    CreditCardRefundManager.RefundCreditCardTransaction(achTransactionWithCC, RefundTotal, portalUser.FullName);

                // Was originally above the previous line of code. Moved it down because it is always null unless the refund/void happens first.
                CCRefund ccRefund = CreditCardRefundManager.GetApprovedRefund(strKey.ToString());

                log.Info(x => x("Refund {0} with response of:{1} for strkey: {2} {3}", achTransactionWithCC.TransTypeValue == TransactionType.DebitPinAcculynk ? Acculynk : CC, response.Result, strKey, DateTime.Now.ToString(CultureInfo.CurrentCulture)));
                var ccRefundTypeType = ccRefund != null ? ccRefund.RefundType.Trim().ToLowerInvariant() : string.Empty;

                if (response.Result == "0")
                {
                    try
                    {
                        // If CCDPPId present then client needs to be debited. 
                        // The flag does not guarantee that we debit the client as well as the CCDPPId
                        if ((!string.IsNullOrWhiteSpace(achTransactionWithCC.CCDPPDId)) || achTransactionWithCC.TransTypeValue == TransactionType.DebitPinAcculynk)
                        {
                            if (ccRefundTypeType != "v")
                            {
                                log.Info(x => x("Transaction is not both Approved and of type V, calling SubmitNewSingleClientTransaction: {0} {1}", strKey, DateTime.Now.ToString(CultureInfo.CurrentCulture)));
                                //updates the tblClientTransHistory and tblClientTransLog tables
                                // Refactored : Same procedure using for Refunds
                                ClientTransactionSubmissionAgent.SubmitClientTransactionAndCreateSettlementRecord(achTransactionWithCC, PaymentConstants.RefundStatus);
                                log.Info($"Refund PayPal -> Client Transaction and Settlement record finished. StrKey = {achTransactionWithCC.StrKey}.");
                            }
                            else
                            {
                                log.Info(x => x("Transaction is both Approved and of type V, Not calling SubmitNewSingleClientTransaction: {0} {1}", strKey, DateTime.Now.ToString(CultureInfo.CurrentCulture)));
                            }
                        }
                    }
                    catch (ReturnValueException retException)
                    {
                        var retResult = ACHErrorCodeManager.GetErrorDescription(retException.ReturnValue.Replace("^", string.Empty));
                        ECMEmail.SendFailedCCRefundChargeBackEmail(achTransactionWithCC, ecmAccount, string.Concat("Error: ", retResult));
                        throw new UserReadableException(retResult, retException);
                    }
                    catch (Exception ex)
                    {
                        log.Error(x => x("Error occurred; EntityID: {0} || StrKey: {1}", achTransactionWithCC.EntityID, achTransactionWithCC.StrKey), ex);
                        throw new UserReadableException(ex.Message, ex);
                    }

                    // Posting a reversal to C2C (PayPal path)
                    var entityMapping = PortalAccountAgent.GetMappingByEntityId(achTransactionWithCC.EntityID);
                    if (!string.IsNullOrWhiteSpace(entityMapping.C2CId))
                    {
                        new C2CService().PostTransactionToConnect2CoreGateway(C2C.DTOs.CUFXBase.TransactionTypeToC2C.Reversal, strKey, PaymentConstants.RefundStatus, portalUser.FullName, achTransactionWithCC.ccOrigID, portalUser.AccountId);
                    }
                }
            }
            catch (Exception ex)
            {
                log.Error(x => x("Error occurred; EntityID: {0} || StrKey: {1}", achTransactionWithCC.EntityID, achTransactionWithCC.StrKey), ex);
                throw new UserReadableException(ex.Message, ex);
            }
            return string.Empty;
        }

        public async Task<string> SettlementClientEdit(SettlementSubmissionCommand command)
        {
            // Set SelectedAccountId to that of the target transaction in order to verify authorization.
            // If transaction is not found, EntityId will be 0, so authorization against Portal Account Id = 0 will fail.
            // This will also prevent a hacker from being able to "test" random Tracking Numbers to see what might be valid because
            // they will just get an Unauthorized error, not a lookup error.
            ACHClientTransaction achClientTransaction = ACHClientTransactionManager.GetItem(command.TrackingNumber);
            var portalIdOfTransaction = _mappingService.GetPortalAccountForEntityId(int.Parse(achClientTransaction.EntityID));
            CurrentPaymentsUser.SelectedAccountId = portalIdOfTransaction;

            if (command.SettlementSubmissionType == PaymentConstants.EDIT_ACTION.ToUpperInvariant())
            {
                CurrentPaymentsUser.VerifyAuthorizationFor(Common.Enums.FunctionalSpecs.EDIT_PENDING_RETURN_SETTLEMENT);
                return await SettlementClientEdit(command, achClientTransaction);
            }
            else if (command.SettlementSubmissionType == "RESUB")
            {
                CurrentPaymentsUser.VerifyAuthorizationFor(Common.Enums.FunctionalSpecs.RESUBMIT_SETTLEMENT);
                return await SettlementClientReSubmission(command, achClientTransaction);
            }

            return string.Empty;
        }

        /// <summary>
        /// Delete Pending Ach Transacations by tracking number
        /// </summary>
        /// <param name="trackingNumber">Tracking Number</param>
        /// <returns>Void</returns>
        public async Task DeletePendingAchTransaction(int trackingNumber)
        {
            AchTransactionView achTransactionView = _transactionManager.GetAchTransaction(trackingNumber);
            VerifyDeleteAuthorization(achTransactionView);

            var portalUser = await GetPortalUser();
            var paymentsUser = _ecmUserService.GetPaymentsUserForPortalUser(portalUser);
            if (paymentsUser.UserId < 0)
            {
                _userSettingsService.SaveUserSettings(portalUser, 0, 0, string.Empty);
            }

            _transactionManager.DeletePendingAchTransaction(trackingNumber, paymentsUser.FullName, paymentsUser.UserId);
        }

        /// <summary>
        /// Get the transaction status by ccOrigId
        /// </summary>
        /// <param name="ccOrigId"></param>
        /// <returns></returns>
        public string GetTransactionStatus(string ccOrigId)
        {
            var ccDetails = CreditCardTransactionManager.GetItemForReport(ccOrigId);

            return ccDetails?.Status ?? UnknownStatusObtained;
        }

        /// <summary>
        /// Void or Refund Pending ProPay/Pago Transaction by tracking number
        /// </summary>
        /// <param name="gatewayTransactionId"></param>
        /// <param name="merchantProfileId"></param>
        /// <param name="paymentMethodId"></param>
        /// <param name="ccOrigId"></param>
        public void VoidOrRefundPendingTransaction(string gatewayTransactionId, string merchantProfileId, string paymentMethodId, string ccOrigId)
        {
            if (ccOrigId == null)
            { throw new VoidRefundException("Void/Refund failed because ccOrigId is null"); }

            bool isProPay = ccOrigId.StartsWith(PaymentConstants.PROPAY_CC_TRANSACTION_PREFIX);
            var portalAlertService = new PortalAlertService();

            var ccDetails = CreditCardTransactionManager.GetItemForReport(ccOrigId);
            var refundFailureMessage = ValidateVoidRefundRequest(ccOrigId, ccDetails);

            if (!string.IsNullOrWhiteSpace(refundFailureMessage))
            { throw new VoidRefundException($"Api failed to void or refund this transaction, {refundFailureMessage}"); }

            if (string.IsNullOrEmpty(merchantProfileId) || string.IsNullOrEmpty(paymentMethodId) || string.IsNullOrEmpty(gatewayTransactionId))
            { UpdateMethodArguments(ref gatewayTransactionId, ref merchantProfileId, ref paymentMethodId, ccDetails, isProPay); }

            if (!long.TryParse(merchantProfileId, out long lMerchantProfileId))
            { throw new VoidRefundException($"Api failed to void or refund this transaction, merchant profile id in wrong format, not number. merchantProfileId: {merchantProfileId}"); }

            var alertDetailsDto = new AlertDetailsDto()
            {
                Alert_TrackingNumberForLink = ccOrigId,
                PortalAccountId = CurrentPaymentsUser.SelectedAccountId
            };

            CreditCardTransResponse creditCardTransResponse = null;
            bool alreadyRefunded = false;
            try
            {
                portalAlertService.DeleteFailedCardRefundAlert(alertDetailsDto, CurrentPaymentsUser.UserId); // delete existing results
                if (isProPay)
                {
                    VoidRefundTransResult response = SWBC.ProtectPay.Connector.PaymentProcessor.CreditCardRefundProcessor.RefundCreditCardTransaction(paymentMethodId, lMerchantProfileId, gatewayTransactionId);
                    creditCardTransResponse = response?.ToCreditCardTransResponse(ccOrigId);
                    alreadyRefunded = response?.AlreadyRefundedByProPay() ?? false;
                    if (response == null)
                    { throw new VoidRefundException($"Void/Refund response was null for ccOrigID: {ccOrigId}"); }
                }
                else if (ccOrigId.StartsWith(PaymentConstants.PAGO_CC_TRANSACTION_PREFIX))
                {
                    Pago.Connector.Dtos.VoidRefundTransResponse response = CreditCardSubmissionAgent.VoidOrRefundPagoCardTransaction(ccDetails.StrKey, gatewayTransactionId);
                    creditCardTransResponse = response?.ToCreditCardTransResponse(ccOrigId);
                    alreadyRefunded = response?.AlreadyRefundedByPago() ?? false;
                    if (response == null)
                    { throw new VoidRefundException($"Void/Refund response was null for ccOrigID: {ccOrigId}"); }
                }
                else
                {
                    throw new VoidRefundException($"Api failed to void or refund this transaction due to invalid ccOrigId: {ccOrigId}");
                }
            }
            catch (Exception ex)
            {
                HandleVoidRefundException(alertDetailsDto, ex, $"Void/Refund Exception for ccOrigId: {ccOrigId}", true);
            }

            var portalUser = GetCurrentUserFullName();
            bool isRefundSucceeded = (creditCardTransResponse != null) && (creditCardTransResponse.RespMsg == PaymentConstants.REFUND);
            var refundInsMsg = GetRefundInsMsg(creditCardTransResponse, isRefundSucceeded);

            InsertLogAndRefundRecord(ccDetails, alertDetailsDto, portalAlertService, creditCardTransResponse, portalUser, refundInsMsg);

            //--- Make sure we (try to) update transaction status even if TransLog and CCRefund table inserts failed for some reason.
            if (isRefundSucceeded || alreadyRefunded)
            {
                portalAlertService.DeleteAlertFromPortal(Facade.Enums.PaymentsAlertTypes.RefundUpdateFailed, alertDetailsDto, CurrentPaymentsUser.UserId);
                var isUpdateSuccessful = UpdateStatus(ccOrigId, alertDetailsDto, PaymentConstants.RefundStatus);
                C2CPostToCore(ccOrigId, ccDetails, alreadyRefunded, portalUser, alertDetailsDto);

                // If status update was successful and CCDPPId present then client needs to be debited.
                if (isUpdateSuccessful && !string.IsNullOrEmpty(ccDetails.CCDPPDId))
                {
                    //updates the tblClientTransHistory and tblClientTransLog tables
                    ClientTransactionSubmissionAgent.SubmitClientTransactionAndCreateSettlementRecord(ccDetails, PaymentConstants.RefundStatus);
                    log.Info($"Refund -> Client Transaction and Settlement record finished. StrKey = {ccDetails.StrKey}.");
                }
            }
            else
            {
                if (creditCardTransResponse.PNRef.StartsWith(PaymentConstants.PAGO_CC_TRANSACTION_PREFIX) && creditCardTransResponse.Result == "51")
                {
                    UpdateStatus(ccOrigId, alertDetailsDto, PaymentConstants.DECLINED);
                }
                else
                {
                    // After inserting the attempt in the table we throw the exception if failed
                    HandleVoidRefundException(alertDetailsDto, null, $"Void/Refund is not already refunded or successful for ccOrigId: {ccOrigId}", false);
                }
            }
        }

        private void C2CPostToCore(string ccOrigId, ACHTransactionWithCC ccDetails, bool alreadyRefunded, string portalUser, AlertDetailsDto dto)
        {
            try
            {
                var entityMapping = PortalAccountAgent.GetMappingByEntityId(ccDetails.EntityID);
                if (!string.IsNullOrWhiteSpace(entityMapping.C2CId) && !alreadyRefunded)
                {
                    new C2CService().PostTransactionToConnect2CoreGateway(C2C.DTOs.CUFXBase.TransactionTypeToC2C.Reversal, Convert.ToInt32(ccDetails.StrKey),
                        PaymentConstants.RefundStatus, portalUser, ccOrigId, CurrentPaymentsUser.SelectedAccountId);
                }
            }
            catch (Exception ex)
            {
                HandleVoidRefundException(dto, ex, $"Void/Refund trying to update status for ccOrigId: {ccOrigId}", true);
            }
        }

        private void UpdateMethodArguments(ref string gatewayTransactionId, ref string merchantProfileId, ref string paymentMethodId, ACHTransactionWithCC ccDetails, bool isProPay)
        {
            var detailsMatch = _transactionProcessingDetailService.GetTransactionProcessingDetail(ccDetails.StrKey, ccDetails.ccOrigID);
            if (detailsMatch != null)
            {
                //ProPay/Pago CC Transaction
                gatewayTransactionId = isProPay ? detailsMatch.TransactionId : detailsMatch.TransactionHistoryId;
                merchantProfileId = detailsMatch.MerchantProfileId;
                paymentMethodId = detailsMatch.PaymentMethodId;
            }
        }

        private bool UpdateStatus(string ccOrigId, AlertDetailsDto alertDetailsDto, string status)
        {
            try
            {
                CreditCardTransactionManager.UpdateStatus(ccOrigId, status);
            }
            catch (Exception ex)
            {
                HandleVoidRefundException(alertDetailsDto, ex, $"Void/Refund trying to update status for ccOrigId: {ccOrigId}", true);
            }
            return true;
        }

        private static void HandleVoidRefundException(AlertDetailsDto alertDetailsDto, Exception ex, string message, bool sendAlert)
        {
            var portalAlertService = new PortalAlertService();
            if (sendAlert)
            { portalAlertService.PostAlertToPortalNoException(Facade.Enums.PaymentsAlertTypes.RefundUpdateFailed, alertDetailsDto); }

            if (ex != null)
            { log.Error(message, ex); }
            else
            { log.Error(message); }
            throw new VoidRefundException(message);
        }

        private static void InsertLogAndRefundRecord(ACHTransactionWithCC ccDetails, AlertDetailsDto alertDetailsDto,
                PortalAlertService portalAlertService, CreditCardTransResponse creditCardTransResponse, string portalUser, string refundInsMsg)
        {
            try
            {
                if (ccDetails == null || creditCardTransResponse == null)
                    throw new VoidRefundException("Void/Refund ccDetails or creditCardTransResponse was null");
                ACHTransactionLogManager.Insert(ccDetails.StrKey, portalUser, DateTime.Now, PaymentConstants.REFUND, refundInsMsg, "");// Insert into tblCCRefunds
                CreditCardRefundManager.InsertRefundAttempt(creditCardTransResponse, ccDetails.StrKey, RefundTotal, portalUser); // Insert into tblTransLog
            }
            catch (Exception ex)
            {
                var errorMsg = $"Void/refund error while trying to insert into either tblTransLog or tblCCRefund. StrKey = {ccDetails.StrKey}.";
                log.Error(errorMsg, ex);
                portalAlertService.PostAlertToPortalNoException(Facade.Enums.PaymentsAlertTypes.RefundUpdateFailed, alertDetailsDto);
                throw new VoidRefundException(errorMsg);
            }
        }

        private static string GetRefundInsMsg(CreditCardTransResponse creditCardTransResponse, bool refundSucceded)
        {
            string refundInsMsg;
            bool isPago = creditCardTransResponse.PNRef.StartsWith(PaymentConstants.PAGO_CC_TRANSACTION_PREFIX);
            switch (refundSucceded)
            {
                case bool _ when creditCardTransResponse == null:
                    refundInsMsg = "[Internal] An error was encountered during void/refund process.";
                    break;
                case bool _ when refundSucceded:
                    refundInsMsg = "Refund Successful " + DateTime.Now.ToLongTimeString() + "";
                    break;
                case bool _ when (isPago && (creditCardTransResponse.Result == "51" || creditCardTransResponse.Result == "54")):
                    refundInsMsg = "Failed";
                    break;
                case bool _ when !refundSucceded:
                    refundInsMsg = "Refund Failed: " + creditCardTransResponse.Result + " - " + creditCardTransResponse.RespMsg + "";
                    break;
                default:
                    refundInsMsg = string.Empty;
                    break;
            }

            return refundInsMsg;
        }

        /// <summary>
        /// Verifies whether Current Payments user has Workflow Privilege or not
        /// </summary>
        /// <returns>Returns True, if CurrentPayments User has WorkflowPrivilege.</returns>
        public bool IsCurrentPaymentsUserWorkflowPrivilege()
        {
            return CurrentPaymentsUser.HasPrivilege(Privileges.SimpleWorkflow.Read);
        }

        private void VerifyDeleteAuthorization(AchTransactionView achTransactionView)
        {
            var ecmAccount = ECMAccountManager.GetItem(achTransactionView.EntityID);
            var dataInfo = new DataAccessAuthInfo()
            {
                CreatedByUserId = _userSettingsService.GetPortalUserIdFor(int.Parse(achTransactionView.SubmittedBy), CurrentPaymentsUser.SelectedAccountId),
                RecordPortalAccountId = _mappingService.GetPortalAccountForEntityId(int.Parse(achTransactionView.EntityID)),
                CurrentUserAccountId = CurrentPaymentsUser.SelectedAccountId,
                RecordPrimaryKey = achTransactionView.StrKey,
                DataValues = new List<KeyValuePair<string, string>>()
                    {
                        new KeyValuePair<string, string>(PaymentConstants.Status, achTransactionView.Status),
                        new KeyValuePair<string, string>(PaymentConstants.ClientCutOffTime, ecmAccount.GenPaymentRunTime.ToString("HH:mm")),
                        new KeyValuePair<string, string>(PaymentConstants.SettlementDate, achTransactionView.EffectiveDate.ToShortDateString())
                    }
            };
            CurrentPaymentsUser.VerifyAuthorizationFor(Common.Enums.DataFunctionSpecs.DELETE_PENDING_PAYMENT, dataInfo);
        }

        private async Task<UserView> GetPortalUser()
        {
            return await _portalService.GetPortalUserInfoByAccountAndId(CurrentPaymentsUser.SelectedAccountId, CurrentPaymentsUser.UserId, UserSettingsRelatedPrivileges.PrivList);
        }

        /// <summary>
        /// Approve Deny Pending Ach Data Info by tracking number
        /// </summary>
        /// <param name="trackingNumber">Tracking Number</param>
        /// <returns>Data Access Auth Info object</returns>
        private async Task<DataAccessAuthInfo> ApproveDenyPendingAchDataInfo(int trackingNumber)
        {
            var paymentsUser = await _ecmUserService.GetPaymentsUserForCurrentPortalUser(CurrentPaymentsUser.SelectedAccountId);
            AchTransactionView achTransactionView = _transactionManager.GetAchTransaction(trackingNumber);
            return new DataAccessAuthInfo()
            {
                CreatedByUserId = int.Parse(achTransactionView.SubmittedBy),
                RecordPortalAccountId = _mappingService.GetPortalAccountForEntityId(int.Parse(achTransactionView.EntityID)),
                CurrentUserAccountId = CurrentPaymentsUser.SelectedAccountId,
                RecordPrimaryKey = achTransactionView.StrKey,
                DataValues = new List<KeyValuePair<string, string>>()
                    {
                        new KeyValuePair<string, string>(Constants.TRACKINGNUMBER, Convert.ToString(achTransactionView.TrackingNumber)),
                        new KeyValuePair<string, string>(Constants.CURRENTECMUSERID, Convert.ToString(paymentsUser.UserId))
                    }
            };
        }

        public async Task ApprovePendingAchTransaction(ApproveDenyPendingAchTransactionCommand command)
        {
            var dataInfo = await ApproveDenyPendingAchDataInfo(command.TrackingNumber);
            try
            {
                if (command.AchTransactionPendingType?.ToLowerInvariant() == PaymentConstants.PendingNameMismatch.ToLowerInvariant())
                {
                    CurrentPaymentsUser.VerifyAuthorizationFor(Common.Enums.DataDrivenFunctionSpecs.APPROVE_OR_DENY_HOLD_NAMEMISMATCH_ACH, dataInfo, _transactionManager);
                    _transactionManager.ApprovePendingNameMismatchAchTransaction(command.TrackingNumber, GetCurrentUserFullName(), command.PortalAccountId);
                }
                if (command.AchTransactionPendingType?.ToLowerInvariant() == PaymentConstants.PendingHighValue.ToLowerInvariant())
                {
                    CurrentPaymentsUser.VerifyAuthorizationFor(Common.Enums.DataDrivenFunctionSpecs.APPROVE_OR_DENY_HOLD_HIGHVALUE_ACH, dataInfo, _transactionManager);
                    _transactionManager.ApprovePendingHighValueAchTransaction(command.TrackingNumber, GetCurrentUserFullName(), command.PortalAccountId);
                }
                if (command.AchTransactionPendingType?.ToLowerInvariant() == PaymentConstants.PendingCredit.ToLowerInvariant())
                {
                    CurrentPaymentsUser.VerifyAuthorizationFor(Common.Enums.DataDrivenFunctionSpecs.APPROVE_OR_DENY_HOLD_CREDIT_ACH, dataInfo, _transactionManager);
                    _transactionManager.ApprovePendingCreditAchTransaction(command.TrackingNumber, GetCurrentUserFullName(), command.PortalAccountId);
                }
            }
            catch (Exception ex)
            {
                throw new UserReadableException($"Error occured while Approving the {command.AchTransactionPendingType} ACH Transaction.", ex);
            }
            var portalAlertService = new PortalAlertService();
            portalAlertService.DeleteHoldAchAllAlerts(command.TrackingNumber, command.PortalAccountId, CurrentPaymentsUser.UserId);
        }

        public AlertsView GetHomeView(int selectedEntityId)
        {
            var view = new AlertsView(GetMenuPermissions(_mappingService.GetDataAccessAuthInfo(selectedEntityId)));
            view.IsUnusualActivityReviewer = CurrentPaymentsUser.HasAuthorizationFor(Common.Enums.FunctionalSpecs.UNUSUAL_ACTIVITY);
            bool.TryParse(ConfigurationManager.AppSettings.Get("EmergencySearchShutoff"), out bool disableRecentPayments);
            view.DisableRecentPayments = disableRecentPayments;
            return view;
        }

        public IvrPaymentsSupportView GetIvrPaymentsSupportView(int selectedEntityId)
        {
            return new IvrPaymentsSupportView(GetMenuPermissions(_mappingService.GetDataAccessAuthInfo(selectedEntityId)))
            {
                IsUnusualActivityReviewer = CurrentPaymentsUser.HasAuthorizationFor(Common.Enums.FunctionalSpecs.UNUSUAL_ACTIVITY),
                StatesList = _addressService.GetAllStates(),
                EnableConFeeStateOfResidence = GetEnableConFeeResponseLogicFlag()
            };
        }

        private string GetCurrentUserFullName()
        {
            var user = _portalService.GetPortalUserInfoByAccountAndIdSync(CurrentPaymentsUser.SelectedAccountId, CurrentPaymentsUser.UserId, new string[] { });
            return user.FullName;
        }

        public async Task DenyPendingAchTransaction(ApproveDenyPendingAchTransactionCommand command)
        {
            try
            {
                // mods to the deny pending calls to use new method
                var dataInfo = await ApproveDenyPendingAchDataInfo(command.TrackingNumber);
                string achPendingType = command.AchTransactionPendingType?.ToLowerInvariant();
                string currentUserFullName = GetCurrentUserFullName();
                if (achPendingType == PaymentConstants.PendingNameMismatch.ToLowerInvariant())
                {
                    CurrentPaymentsUser.VerifyAuthorizationFor(Common.Enums.DataDrivenFunctionSpecs.APPROVE_OR_DENY_HOLD_NAMEMISMATCH_ACH, dataInfo, _transactionManager);
                    _transactionManager.DenyPendingTransactionsByType(command.TrackingNumber, CurrentPaymentsUser.UserId, command.PortalAccountId, Facade.Enums.PaymentsAlertTypes.NameMismatch, currentUserFullName);
                }
                else if (achPendingType == PaymentConstants.PendingHighValue.ToLowerInvariant())
                {
                    CurrentPaymentsUser.VerifyAuthorizationFor(Common.Enums.DataDrivenFunctionSpecs.APPROVE_OR_DENY_HOLD_HIGHVALUE_ACH, dataInfo, _transactionManager);
                    _transactionManager.DenyPendingTransactionsByType(command.TrackingNumber, CurrentPaymentsUser.UserId, command.PortalAccountId, Facade.Enums.PaymentsAlertTypes.HighValueTransaction, currentUserFullName);
                }
                else if (achPendingType == PaymentConstants.PendingCredit.ToLowerInvariant())
                {
                    CurrentPaymentsUser.VerifyAuthorizationFor(Common.Enums.DataDrivenFunctionSpecs.APPROVE_OR_DENY_HOLD_CREDIT_ACH, dataInfo, _transactionManager);
                    _transactionManager.DenyPendingTransactionsByType(command.TrackingNumber, CurrentPaymentsUser.UserId, command.PortalAccountId, Facade.Enums.PaymentsAlertTypes.PendingCredit, currentUserFullName);
                }
            }
            catch (Exception ex)
            {
                throw new UserReadableException($"Error occured while Denying the {command.AchTransactionPendingType} ACH Transaction.", ex);
            }
            var portalAlertService = new PortalAlertService();
            portalAlertService.DeleteHoldAchAllAlerts(command.TrackingNumber, command.PortalAccountId, CurrentPaymentsUser.UserId);
        }

        public void DeleteChargebackAlerts(DeleteChargebackAlertsCommand command)
        {
            var portalAlertService = new PortalAlertService();
            var alertDetailsDto = new AlertDetailsDto()
            {
                PortalAccountId = command.PortalAccountId,
                Alert_TrackingNumberForLink = command.TrackingNumber
            };
            alertDetailsDto.ChargebackEmailDto = new Facade.Dtos.ChargebackEmailDto();
            alertDetailsDto.ChargebackEmailDto.LinkToTransactionDetailsPage = alertDetailsDto.ToLinkUrl();

            portalAlertService.DeleteChargebackAllAlerts(alertDetailsDto, CurrentPaymentsUser.UserId);
        }

        private async Task<string> SettlementClientReSubmission(SettlementSubmissionCommand command, ACHClientTransaction oldClientTrans)
        {
            const string SettlementGeneralLedgerType = "G";
            string result = string.Empty;
            string systemCodeToPass = string.Empty;
            string secCodeToPass = string.Empty;
            string trackingNumberPrefix = oldClientTrans.TrackingNumber.ToUpperInvariant().GetLetters();
            int entityId = 0;
            var portalUser = await GetPortalUser();

            if (!RoutingNumberManager.IsValidRoutingNumber(command.RoutingNumber))
            {
                throw new UserReadableException("Routing Number is invalid.");
            }

            var ecmAccount = ECMAccountManager.GetItem(oldClientTrans.EntityID);
            if (ecmAccount != null)
            {
                int.TryParse(ecmAccount.EntityID, out entityId);

                if (IsKnownSettlementTransPrefix(trackingNumberPrefix))
                {
                    systemCodeToPass = PaymentConstants.AS400CREDIT;
                }
                else if (IsKnownReversalTransPrefix(trackingNumberPrefix))
                {
                    systemCodeToPass = PaymentConstants.AS400DEBIT;
                }
                else
                {
                    systemCodeToPass = (oldClientTrans.Amount < 0 ? PaymentConstants.AS400DEBIT : PaymentConstants.AS400CREDIT);
                }

                switch (command.AccountType.ToUpperInvariant())
                {
                    case SettlementGeneralLedgerType:
                        secCodeToPass = PaymentConstants.ACH_SETTLEMENT_BATCH_TYPE;
                        break;
                    default:
                        secCodeToPass = PaymentConstants.LOAN_LEVEL_CREDIT_CARD_SETTLEMENT_PREFIX;
                        break;
                }

                try
                {
                    POCORepo.ClientTransHistory clientTrans = ClientTransactionSubmissionAgent.SubmitNewSingleClientTransaction(entityId, TransactionType.ClientResubmission,
                        oldClientTrans.Amount, command.RoutingNumber,
                        command.AccountType, command.AccountNo,
                        secCodeToPass, oldClientTrans.IndividualName,
                        PaymentConstants.ORIG_APPID, systemCodeToPass,
                        ecmAccount.ACHCustomerNumber, string.Empty, DateTime.Now, PaymentSource.OnlineECM);

                    POCORepo.ClientResubmitTransHistory newClientTrans = ClientTransactionSubmissionAgent.InsertClientResubmitTransaction(clientTrans, oldClientTrans);

                    ACHClientTransactionLogManager.Insert(oldClientTrans.StrKey, portalUser.FullName, DateTime.Now, PaymentConstants.RESUBMITTED_ACTION, String.Format("Transaction resubmitted. New Tracking Number: {0}", newClientTrans.ResubmitTrackingNumber));
                    ACHClientTransaction tmpClientTrans = ACHClientTransactionManager.GetItem(clientTrans.TrackingNumber);
                    if (!ACHClientTransactionLogManager.Insert(tmpClientTrans.StrKey, portalUser.FullName, DateTime.Now, PaymentConstants.SUBMITTED_ACTION, command.Comments))
                    {
                        throw new Exception("Transaction has been submitted but the comments were not stored. please contact support team.");
                    }
                }
                catch (ReturnValueException ex)
                {
                    result = GetAchError(ex.ReturnValue);
                    log.Error(result, ex);
                    throw new UserReadableException(result, ex);
                }
                catch (Exception ex)
                {
                    throw new UserReadableException(ex.Message, ex);
                }
            }
            else
            {
                throw new UserReadableException("Unable to submit transaction. The transaction entity is not found.");
            }

            return result;
        }

        private async Task<string> SettlementClientEdit(SettlementSubmissionCommand command, ACHClientTransaction achClientTransaction)
        {
            var portalUser = await GetPortalUser();
            if (!RoutingNumberManager.IsValidRoutingNumber(command.RoutingNumber))
            {
                throw new UserReadableException("Routing Number is invalid.");
            }

            var result = GetAchError(ACHClientTransactionManager.Update(achClientTransaction.TrackingNumber, command.Amount, command.RoutingNumber, command.AccountType, command.AccountNo));
            if (string.IsNullOrEmpty(result.Trim()))
            {
                if (!ACHClientTransactionLogManager.Insert(achClientTransaction.StrKey, portalUser.FullName, DateTime.Now, PaymentConstants.EDIT_ACTION, string.Format(SettlementComments, command.Comments, string.Join(" - ", command.EditedFields))))
                {
                    throw new UserReadableException("Unable to save client transaction log, please contact support team.");
                }
            }
            else
            {
                throw new UserReadableException(result);
            }

            return string.Empty;
        }

        private string GetAchError(string response)
        {
            string retResult = string.Empty;
            if (string.IsNullOrEmpty(response.Trim()))
            {
                return retResult;
            }

            if (response.Contains("^"))
            {
                int errorCode;
                int.TryParse(response.Trim(CapCharSymbol), out errorCode);
                ACHErrorCode errorMsg = ACHErrorCodeManager.GetItem(errorCode);
                return errorMsg.ErrorDescription;
            }
            else if (response == "1")
            {
                retResult = string.Empty;
            }
            else
            {
                retResult = "Invalid response";
            }

            return retResult;
        }

        private bool IsKnownSettlementTransPrefix(string prefix)
        {
            return prefix == PaymentConstants.ACH_SETTLEMENT_BATCH_TYPE
                || prefix == PaymentConstants.LOAN_LEVEL_ACH_SETTLEMENT_PREFIX
                || prefix == PaymentConstants.LOAN_LEVEL_CREDIT_CARD_SETTLEMENT_PREFIX
                || prefix == PaymentConstants.WEB
                || prefix == PaymentConstants.ClientLoanLevelPrefix;
        }

        private bool IsKnownReversalTransPrefix(string prefix)
        {
            return prefix == PaymentConstants.REVERSAL_TRANSACTION_PREFIX
                || prefix == PaymentConstants.RETURNS_TRANSACTION_PREFIX
                || prefix == PaymentConstants.REFUND_TRANSACTION_PREFIX
                || prefix == PaymentConstants.CB;
        }

        private string ValidateVoidRefundRequest(string ccOrigId, ACHTransactionWithCC ccDetails)
        {
            var result = string.Empty;
            int iStrKey = 0;
            if (ccDetails == null)
            {
                result = $"did not find a matching record for ccOrigId: {ccOrigId}";
            }
            else if (!int.TryParse(ccDetails.StrKey, out iStrKey))
            {
                result = $"strkey is in wrong format: {ccDetails.StrKey}";
            }
            else
            {
                result = ValidateAndSetVoidOrRefund(ccDetails.Status, ccDetails.EffectiveDate, Constants.CC_TRANSACTION, iStrKey, ccDetails.EntityID, ccOrigId);
            }

            return result;
        }

        private string ValidateAndSetVoidOrRefund(string transactionStatus, DateTime originationDate, string transType, int strKey, string entityId, string ccOrigId)
        {
            var ecmAccount = ECMAccountManager.GetItem(entityId);
            var result = string.Empty;
            if (_transactionProcessingDetailService.GetTransactionProcessingDetail(strKey.ToString(), ccOrigId) == null)
            {
                //This is not a ProPay/Pago CC return false
                result = Resources.Messages.InvalidCreditCardNoTrackingTableMatch;
            }
            else if (_validationService.PerformReversalOrVoidRefund(transactionStatus, originationDate, transType, strKey, ecmAccount.SWBCHandlesCC, ecmAccount.GenPaymentRunTime))
            {
                // if passes these tests, it is okay
                result = string.Empty;
            }
            else if (!CurrentPaymentsUser.HasAuthorizationFor(Common.Enums.FunctionalSpecs.REFUND_CC_PAYMENT))
            {
                result = Resources.Messages.RefundFailInsufficientPermissions;
            }
            else if (transactionStatus.ToUpperInvariant() == PaymentConstants.FUNDED.ToUpperInvariant()
                && _dateUtility.GetNumberOfWorkingDaysUntilDate(originationDate, transType) > PaymentConstants.RefundMaxAllowedDays)
            {
                result = Resources.Messages.RefundPeriodPastMaxDays;
            }
            else
            {
                result = Resources.Messages.RefundGeneralError;
            }

            return result;
        }

        public async Task<string> FinalizePayment(int strKey, string status, string newTrackingNumber)
        {
            CurrentPaymentsUser.VerifyAuthorizationFor(Common.Enums.FunctionalSpecs.FINALIZE_CARDPAYMENT_STATUS);
            var portalUser = await GetPortalUser();
            var userFullName = portalUser.FullName;
            int entityId;

            var isParametersValid = ValidateParametersToFinalize(status, newTrackingNumber, strKey, out string errorMsg);
            if (!isParametersValid) { return errorMsg; }

            try
            {
                using (var unitOfWork = new ACHTransUOW(true))
                {
                    POCORepo.TransHistory transByCcOrigId = Repositories.TransHistoryRepository.GetByCreditCardOriginalId(unitOfWork, newTrackingNumber);
                    POCORepo.TransHistory trans = Repositories.TransHistoryRepository.GetByKey(unitOfWork, strKey);
                    POCORepo.Notification notification = Repositories.NotificationRepository.GetByKey(unitOfWork, strKey);

                    var originalTrackingNumber = trans.ccOrigID;
                    entityId = trans.EntityID;
                    var isTransValid = ValidateTransactionToFinalize(transByCcOrigId, trans, newTrackingNumber, out errorMsg);
                    if (!isTransValid) { return errorMsg; }
                    trans.Status = status;
                    if (notification != null) { UpdateNotificationRepo(status, unitOfWork, notification); }
                    if (status.ToUpperInvariant() != "DELETED") { UpdateSplitPymtRepo(trans, newTrackingNumber, unitOfWork); }

                    Repositories.TransHistoryRepository.Update(unitOfWork, trans);
                    if (status.ToUpperInvariant() != "DELETED")
                    {
                        Repositories.TransLogRepository.Add(unitOfWork, BuildTransLogForTrackNumUpdate(originalTrackingNumber, newTrackingNumber, userFullName, trans));
                    }
                    Repositories.TransLogRepository.Add(unitOfWork, BuildTransLogForStatusUpdate(status, userFullName, trans));
                    unitOfWork.SaveChanges();
                }
            }
            catch (Exception ex)
            {
                log.Error(GetStatusUpdateErrorMsg(strKey, newTrackingNumber), ex);
                throw;
            }

            // If entity is enabled for C2C, attempt to post to core based on new status.  Otherwise, return out of method.
            var mapping = _mappingCache.GetMappingByEntityId(entityId);
            if (string.IsNullOrWhiteSpace(mapping?.C2CId))
            {
                return errorMsg;
            }
            
            ProcessC2CForUpdatedStatus(status, strKey, userFullName, newTrackingNumber, portalUser.AccountId, mapping.PortalId);

            return errorMsg;
        }

        private bool ValidateParametersToFinalize(string status, string newTrackingNumber, int strKey, out string errorMsg)
        {
            errorMsg = string.Empty;

            if (status.ToUpperInvariant() != "DELETED" && !Regex.IsMatch(newTrackingNumber, "^[a,v,A,V]([a-zA-Z0-9]{11,24})$"))
            {
                errorMsg = "Please enter a valid Tracking Number from the payment processor to associate with this transaction. A valid tracking number starts with the letter \"V\" or \"A\" and contains 12 - 25 alpha-numeric characters.";
                return false;
            }

            if (strKey <= 0)
            {
                errorMsg = "Invalid StrKey provided.";
                return false;
            }

            return true;
        }

        private bool ValidateTransactionToFinalize(POCORepo.TransHistory transByCcOrigId, POCORepo.TransHistory transByStrKey, string newTrackingNumber, out string errorMsg)
        {
            errorMsg = string.Empty;

            if (transByCcOrigId != null)
            {
                errorMsg = string.Format("Tracking Number <i>{0}</i> is already in use by other transaction.", newTrackingNumber);
                return false;
            }

            if (transByStrKey.Status.ToUpperInvariant() != PaymentConstants.PENDING.ToUpperInvariant())
            {
                errorMsg = "Status is already updated for this Tracking Number.";
                return false;
            }

            return true;
        }

        private POCORepo.TransLog BuildTransLogForTrackNumUpdate(string origTrackNum, string newTrackNum, string userFullName, POCORepo.TransHistory trans, string salesforceNumber = null)
        {
            var comments = $"Tracking Number updated from {origTrackNum} to {newTrackNum}";
            var commentEnd = salesforceNumber == null ? "." : $" for Salesforce ticket number: {salesforceNumber}.";
            comments = string.Concat(comments, commentEnd);
            return BuildTransLog(userFullName, comments, trans);
        }

        private POCORepo.TransLog BuildTransLogForStatusUpdate(string status, string userFullName, POCORepo.TransHistory trans)
        {
            var comments = $"Status updated from Pending to {status}.";
            return BuildTransLog(userFullName, comments, trans);
        }

        private POCORepo.TransLog BuildTransLogForManualUpdate(string oldStatus, string newStatus, string userFullName, string newSalesforceNum, POCORepo.TransHistory trans)
        {
            var comments = $"Status updated from {oldStatus} to {newStatus} for Salesforce ticket number: {newSalesforceNum}.";
            return BuildTransLog(userFullName, comments, trans);
        }
        
        private POCORepo.TransLog BuildTransLog(string userFullName, string comments, POCORepo.TransHistory trans)
        {
            return new POCORepo.TransLog()
            {
                ActionBy = userFullName,
                ActionDate = DateTime.Now,
                ActionTaken = "Updated",
                Comments = comments,
                RIDNumber = string.Empty,
                TransHistory = trans
            };
        }

        private void UpdateNotificationRepo(string status, ACHTransUOW unitOfWork, POCORepo.Notification notification)
        {
            if (IsTransactionStatusUnsuccessful(status))
            {
                Repositories.NotificationRepository.Remove(unitOfWork, notification);
            }
            else
            {
                notification.PrintStatus = "Pending";
                Repositories.NotificationRepository.Update(unitOfWork, notification);
            }
        }

        private void UpdateSplitPymtRepo(POCORepo.TransHistory trans, string newTrackingNumber, ACHTransUOW unitOfWork)
        {
            trans.ccOrigID = newTrackingNumber;
            foreach (POCORepo.SplitPayment splitPayment in trans.SplitPayments)
            {
                splitPayment.TrackingNumber = newTrackingNumber;
                Repositories.SplitPaymentRepository.Update(unitOfWork, splitPayment);
            }
        }

        public async Task<string> ManualCCUpdate(int strKey, string newStatus, string newTrackingNumber, string salesforceNumber)
        {
            CurrentPaymentsUser.VerifyAuthorizationFor(Common.Enums.FunctionalSpecs.MANUALLY_UPDATE_CARD_PAYMENT);
            var portalUser = await GetPortalUser();
            var userFullName = portalUser.FullName;
            int entityId;

            var isParametersValid = ValidateParametersToFinalize(newStatus, newTrackingNumber, strKey, out string errorMsg);
            if (!isParametersValid) { return errorMsg; }

            try
            {
                using (var unitOfWork = new ACHTransUOW(true))
                {
                    var trans = Repositories.TransHistoryRepository.GetByKey(unitOfWork, strKey);
                    if (trans == null) { return "Transaction not found."; }
                    var originalTrackingNumber = trans.ccOrigID;
                    entityId = trans.EntityID;
                    bool isTrackingNumberUpdated = originalTrackingNumber != newTrackingNumber;
                    if (isTrackingNumberUpdated && Repositories.TransHistoryRepository.GetByCreditCardOriginalId(unitOfWork, newTrackingNumber) != null)
                    {
                        return "The new tracking number already exists on another transaction.";
                    }
                    var oldStatus = trans.Status;
                    var isStatusUpdated = oldStatus.ToUpperInvariant() != newStatus.ToUpperInvariant();
                    if (!isTrackingNumberUpdated && !isStatusUpdated)
                    {
                        return "The Tracking Number and/or Status must be a different value.";
                    }
                    trans.Status = newStatus;
                    if (isTrackingNumberUpdated)
                    {
                        UpdateSplitPymtRepo(trans, newTrackingNumber, unitOfWork);
                        Repositories.TransLogRepository.Add(unitOfWork, BuildTransLogForTrackNumUpdate(originalTrackingNumber, newTrackingNumber, userFullName, trans, salesforceNumber));
                    }
                    Repositories.TransHistoryRepository.Update(unitOfWork, trans);
                    if (oldStatus.ToUpperInvariant() != newStatus.ToUpperInvariant()) { Repositories.TransLogRepository.Add(unitOfWork, BuildTransLogForManualUpdate(oldStatus, newStatus, userFullName, salesforceNumber, trans)); }
                    unitOfWork.SaveChanges();
                }
            }
            catch (Exception ex)
            {
                log.Error(GetStatusUpdateErrorMsg(strKey, newTrackingNumber), ex);
                throw;
            }
            return errorMsg;
        }

        private void ProcessC2CForUpdatedStatus(string newStatus, int strKey, string userFullName, string newTrackingNumber, int accountId, int portalId)
        {
            var c2cService = new C2CService();
            var statusUpper = newStatus.ToUpperInvariant();

            if (statusUpper == PaymentConstants.APPROVED.ToUpperInvariant() || statusUpper == PaymentConstants.FUNDED.ToUpperInvariant())
            {
                c2cService.PostTransactionToConnect2CoreGateway(C2C.DTOs.CUFXBase.TransactionTypeToC2C.Payment, strKey, newStatus, userFullName, newTrackingNumber, accountId);
            }
            else if (statusUpper == PaymentConstants.DECLINED.ToUpperInvariant() || statusUpper == PaymentConstants.DELETED.ToUpperInvariant())
            {
                c2cService.UpdatePostToCoreStatusByStrKey(strKey.ToString(), userFullName, C2CPostStatus.NotApplicable, portalId);
                C2CTransactionAgent.CreateTransLogEntry($"C2C messages have been set to not applicable because the transaction is {newStatus}.", strKey);
            }
        }
        
        private string GetStatusUpdateErrorMsg(int strKey, string newTrackingNumber)
        {
            return $"Error updating transaction in database while finalizing payment. StrKey: {strKey}, TrackingNumberToUpdate: {newTrackingNumber}";
        }

        private bool IsTransactionStatusUnsuccessful(string status)
        {
            return status.ToUpperInvariant() == "DELETED" || status.ToUpperInvariant() == "DECLINED";
        }
    }

}
