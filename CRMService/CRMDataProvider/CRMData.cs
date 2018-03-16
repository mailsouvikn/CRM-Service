using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.Dto;
using FHLB.CRMService.Shared;

namespace FHLB.CRMDataProvider
{
    public static class CRMData
    {
        const string _connectionString = "data source=localhost;initial catalog=AzurePOC;integrated security=True;MultipleActiveResultSets=True;App=POCCRMApp";
        const string _querySelectCustomerByCustNum = "Select * From [dbo].[TestCRMSource] Where StatusCode = 'A' AND CustNum=@custnum";
        const string _queryInsertDestination02 = @"IF NOT EXISTS(Select 1 From [dbo].[TestDestinationApp02] Where BatchCustId = @BatchCustId)
                                                BEGIN
                                                UPDATE [dbo].[TestDestinationApp02] SET StatusCode = 'I', ModifiedOn = @ModifiedOn Where CustNum=@custNum AND StatusCode='A' AND (BatchCustId IS NULL OR BatchCustId<@BatchCustId)
                                                INSERT INTO [dbo].[TestDestinationApp02]
                                                ([CustNum],[CurrentAssets],[CreatedOn],[StatusCode],[BatchCustId])
                                                VALUES  (@custNum ,@CurrentAssets,@CreatedOn,@StatusCode,@BatchCustId)
                                                END";
        public static bool LoadDataInCrmTable(DataTable source)
        {
            bool isSuccess = false;
            bool exists = false;
            int rowNum = 0;

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                String queryInsert = @"INSERT INTO [dbo].[TestCRMSource]
                                        ([CustNum] ,[FirstName]  ,[LastName] ,[EmailAddress]  ,[AccountNumber]  ,[CurrentBalance] ,[CurrentAssets] ,[CreatedOn] ,[StatusCode]) 
                                        VALUES (@custnum,@fname, @lname, @email, @acctnum, @curbal, @curasset, @created, @status)";


                string queryUpdate = "Update [dbo].[TestCRMSource] SET StatusCode='I', ModifiedOn=@modifiedon Where CustNum=@custnum AND StatusCode = 'A'";
                connection.Open();

                foreach (DataRow item in source.Rows)
                {
                    rowNum++;
                    exists = false;
                    using (SqlCommand commandSelect = new SqlCommand(_querySelectCustomerByCustNum, connection))
                    {
                        commandSelect.Parameters.AddWithValue("@custnum", item["CustNum"]);

                        exists = !string.IsNullOrEmpty(Convert.ToString(commandSelect.ExecuteScalar()));
                        if (exists)
                        {
                            //delete existing
                            using (SqlCommand commandUpdate = new SqlCommand(queryUpdate, connection))
                            {
                                commandUpdate.Parameters.AddWithValue("@custnum", item["CustNum"]);
                                commandUpdate.Parameters.AddWithValue("@modifiedon", DateTime.Now);
                                int result = commandUpdate.ExecuteNonQuery();

                                // Check Error
                                if (result < 0)
                                    CommonFunctions.LogInConsole("Error inserting data into Database for row# " + rowNum);
                            }

                        }

                        //insert
                        using (SqlCommand commandInsert = new SqlCommand(queryInsert, connection))
                        {
                            commandInsert.Parameters.AddWithValue("@custnum", item["CustNum"]);
                            commandInsert.Parameters.AddWithValue("@fname", item["Fname"]);
                            commandInsert.Parameters.AddWithValue("@lname", item["LName"]);
                            commandInsert.Parameters.AddWithValue("@email", item["Email"]);
                            commandInsert.Parameters.AddWithValue("@acctnum", item["AcctNum"]);
                            commandInsert.Parameters.AddWithValue("@curbal", item["Balance"]);
                            commandInsert.Parameters.AddWithValue("@curasset", item["Asset"]);
                            commandInsert.Parameters.AddWithValue("@created", DateTime.Now);
                            commandInsert.Parameters.AddWithValue("@status", "A");
                            int result = commandInsert.ExecuteNonQuery();

                            // Check Error and log
                            if (result < 0)
                                CommonFunctions.LogInConsole("Error inserting data into Database for row# " + rowNum);
                        }
                    }
                }
                connection.Close();

            }

            CommonFunctions.LogInConsole("Data loaded in CRM Table");
            return isSuccess;
        }

        public static CustomerDto GetCustomerByCustNum(string custnum)
        {
            CustomerDto result = null;
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (SqlCommand commandSelect = new SqlCommand(_querySelectCustomerByCustNum, connection))
                {
                    commandSelect.Parameters.AddWithValue("@custnum", custnum);
                    var reader = commandSelect.ExecuteReader();                   
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var x = Convert.ToString(reader["EmailAddress"]);
                            result = new CustomerDto()
                            {
                                Id = Convert.ToInt64(reader["Id"]),
                                AccountNumber = Convert.ToInt32(reader["AccountNumber"]),
                                Asset = Convert.ToDecimal(reader["CurrentAssets"]),
                                Balance = Convert.ToDecimal(reader["CurrentBalance"]),
                                CustNum = Convert.ToString(reader["CustNum"]),
                                Email = Convert.ToString(reader["EmailAddress"]),
                                FirstName = Convert.ToString(reader["FirstName"]),
                                LastName = Convert.ToString(reader["LastName"]),
                                StatusCode = Convert.ToString(reader["StatusCode"])
                            };
                            break;
                        }
                        
                        reader.Close();
                    }
                }

                connection.Close();
            }
            return result;
        }

        public static int InsertCustomerInDestination02(CustomerDto customer)
        {
            int result = 0;
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                try
                {
                    connection.Open();
                    using (SqlCommand commandInsert = new SqlCommand(_queryInsertDestination02, connection))
                    {
                        commandInsert.Parameters.AddWithValue("@custnum", customer.CustNum);
                        commandInsert.Parameters.AddWithValue("@currentassets", customer.Asset);
                        commandInsert.Parameters.AddWithValue("@ModifiedOn", DateTime.Now);
                        commandInsert.Parameters.AddWithValue("@StatusCode", "A");
                        commandInsert.Parameters.AddWithValue("@BatchCustId", customer.Id);                       
                        commandInsert.Parameters.AddWithValue("@CreatedOn", DateTime.Now);                      
                        result = commandInsert.ExecuteNonQuery();

                        // Check Error and log
                        if (result < 0)
                            CommonFunctions.LogInConsole("Error saving data into Destination02");
                    }
                }
                catch(Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    connection.Close();
                }              
            }
                return result;
        }
    }
}
