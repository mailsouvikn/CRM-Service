using FHLB.AzureServiceManager;
using FHLB.CRMService.Shared;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization.Json;
using Common.Dto;
using System.IO;
using FHLB.CRMDataProvider;

namespace AHPClient
{
    class Program
    {
        const string _topicName = "crmmember";
        const string _subscriptionName = "CrmMemberSubscriptionV2";
        static void Main(string[] args)
        {
            try
            {
                var custDataJson = AzureProvider.GetCustomerDataFromSubscription(_topicName, _subscriptionName);
                CommonFunctions.LogInConsole(custDataJson);
                var customer = CommonFunctions.Deserialize<CustomerDto>(custDataJson);
                var rows = CRMData.InsertCustomerInDestination02(customer);
                if (rows > 0)
                {
                    CommonFunctions.LogInConsole("Customer data id#" + customer.Id + " process!");
                }
                else
                {
                    CommonFunctions.LogInConsole("Customer data id#" + customer.Id + " could not be processed!");
                }
            }
            catch (Exception ex)
            {
                CommonFunctions.LogInConsole(ex.Message);
                CommonFunctions.LogInConsole(ex.StackTrace);
            }
            

        }
    }
}
