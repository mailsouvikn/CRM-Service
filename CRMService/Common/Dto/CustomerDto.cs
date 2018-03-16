using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common.Dto
{
    public class CustomerDto
    {
        public string CustNum { get; set; }
        public long Id { get; set; }
        public int AccountNumber { get; set; }
        public decimal Balance { get; set; }
        public decimal Asset { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; } 
        public string StatusCode { get; set; }        
    }
}
