using FHLB.AzureServiceManager;
using FHLB.CRMDataProvider;
using FHLB.CRMService.Shared;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FHLB.CRMService
{
    class Program
    {
        const string sheetname = "CRMSource";
        static void Main(string[] args)
        {
            try
            {
                CommonFunctions.LogInConsole("POC CRM App Started");
                FileInfo fi = new FileInfo("C:\\MyWork\\SharedRepository\\Customer.csv");
                var path = fi.FullName;
                var data = CommonFunctions.LoadDataFromCSV(path, sheetname);
                CRMData.LoadDataInCrmTable(data);

                AzureProvider.WriteBatchToQueue01(data.Select().ToList().Select(c => "CustNum=" + c["CustNum"].ToString()).ToList());
                CommonFunctions.LogInConsole("Azure Queue notified with customer data update");
                CommonFunctions.LogInConsole("POC CRM App Ended");

            }
            catch(MessagingException mex)
            {
                CommonFunctions.LogInConsole(mex.Message);
                CommonFunctions.LogInConsole("Service Bus not available");
            }
            catch (Exception ex)
            {
                CommonFunctions.LogInConsole(ex.Message);
                CommonFunctions.LogInConsole(ex.StackTrace);
               
            }


        }
    }
}
