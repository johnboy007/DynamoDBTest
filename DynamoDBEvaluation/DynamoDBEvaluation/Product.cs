using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Synergy.FW.DAL;

namespace DynamoDBEvaluation
{
    public class Product : DA_Table
    {
        public Product() : base() { }
        public Product(DB DB) : base(DB) { }
        public Product(ArrayList arDataSet) : base(arDataSet) { }
        public Product(ArrayList arDataSet, DB DB) : base(arDataSet, DB) { }

        public DA_Field_Int Id = new DA_Field_Int("Id", DA_Field_Flag.PrimaryKey, DA_Field_Flag.Normal);
        public DA_Field_String Title = new DA_Field_String("Title", 250, DA_Field_Flag.Normal);
        public DA_Field_String ISBN = new DA_Field_String("ISBN", 250, DA_Field_Flag.Normal);
        public DA_Field_String Authors = new DA_Field_String("Authors", 250, DA_Field_Flag.Normal);
        public DA_Field_Int Price = new DA_Field_Int("Price", DA_Field_Flag.Normal);
        public DA_Field_String Dimensions = new DA_Field_String("Dimensions", 250, DA_Field_Flag.Normal);
        public DA_Field_Int PageCount = new DA_Field_Int("PageCount", DA_Field_Flag.Normal);
        public DA_Field_Bool InPublication = new DA_Field_Bool("InPublication", DA_Field_Flag.Normal);
        public DA_Field_String ProductCategory = new DA_Field_String("ProductCategory", 250, DA_Field_Flag.Normal);

        public override void Initialize()
        {
            base.TableName = "ProductCatalog";
            base.DefaultQuery = "ProductCatalog";
            base.CreateSQL = @"";
            base.AddTableFields(Id, Title, ISBN, Authors, Price, Dimensions, PageCount, InPublication, ProductCategory);
            base.DefaultPagingIdentifier = "ProductCatalog";
        }

        public Product GetProduct(int id)
        {
            Id.Value = id;

            foreach (ArrayList arRow in Get())
            {
                return (new Product(arRow));
            }
            return null;
        }

        public ArrayList GetProducts()
        {
            var products = new ArrayList();
            foreach (ArrayList arRow in Get("SELECT * FROM ProductCatalog"))
            {
                products.Add(new Product(arRow));
            }
            return products;
        }
    }
}
