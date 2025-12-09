namespace SistemaDeAnalisis.Models
{
    public class SalesData
    {
        public int Id { get; set; }
        public int CustomerID { get; set; }
        public string? FirstName { get; set; }
        public string? LastName { get; set; }
        public string? Email { get; set; }
        public int OrderID { get; set; } 
        public int ProductID { get; set; }
        public string? ProductName { get; set; }
        public string? Category { get; set; }
        public decimal Price { get; set; }
        public int Quantity { get; set; }
        public decimal TotalPrice { get; set; }
        public DateTime OrderDate { get; set; }
        public string Source { get; set; } = string.Empty;
        public DateTime CreatedDate { get; set; }
    }
}

public class FactSales
{
    public int OrderID { get; set; }
    public int CustomerID { get; set; }
    public int ProductID { get; set; }
    public DateTime OrderDate { get; set; }
    public int Quantity { get; set; }
    public decimal TotalPrice { get; set; }
}
