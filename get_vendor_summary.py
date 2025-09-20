import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# -----------------------------
# Create vendor summary
# -----------------------------
def create_vendor_summary(conn):
    """
    Merge the different tables to get the overall vendor summary.
    """
    try:
        vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (
           SELECT
              VendorNumber,
              SUM(Freight) AS FreightCost
           FROM vendor_invoice
           GROUP BY VendorNumber
      ),

      PurchaseSummary AS (
        SELECT
           p.VendorNumber,
           p.VendorName,
           p.Brand,
           p.Description,
           p.PurchasePrice,
           pp.Price AS ActualPrice,
           pp.Volume,
           SUM(p.Quantity) AS TotalPurchaseQuantity,
           SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
          ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0  
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),

     SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
     )    

     SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalExciseTax,
        fs.FreightCost
     FROM PurchaseSummary ps 
     LEFT JOIN SalesSummary ss
         ON ps.VendorNumber = ss.VendorNo
         AND ps.Brand = ss.Brand
     LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
     ORDER BY ps.TotalPurchaseDollars DESC""", conn)

        return vendor_sales_summary

    except Exception as e:
        logging.error("Error while creating vendor summary: %s", e)
        raise


# -----------------------------
# Clean the data
# -----------------------------
def clean_data(df):
    """
    Clean and enrich the vendor sales summary data.
    """
    try:
        df['Volume'] = df['Volume'].astype(float)

        # Fill missing values with 0
        df.fillna(0, inplace=True)

        # Remove whitespace from string columns
        df['VendorName'] = df['VendorName'].str.strip()
        df['Description'] = df['Description'].str.strip()

        # Create new metrics
        df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']

        # Avoid division by zero
        df['ProfitMargine'] = df.apply(
            lambda row: (row['GrossProfit'] / row['TotalSalesDollars'] * 100)
            if row['TotalSalesDollars'] != 0 else 0,
            axis=1
        )
        df['StockTurnover'] = df.apply(
            lambda row: (row['TotalSalesQuantity'] / row['TotalPurchaseQuantity'])
            if row['TotalPurchaseQuantity'] != 0 else 0,
            axis=1
        )
        df['SalestoPurchaseRatio'] = df.apply(
            lambda row: (row['TotalSalesDollars'] / row['TotalPurchaseDollars'])
            if row['TotalPurchaseDollars'] != 0 else 0,
            axis=1
        )

        return df

    except Exception as e:
        logging.error("Error while cleaning data: %s", e)
        raise


# -----------------------------
# Main script
# -----------------------------
if __name__ == '__main__':
    try:
        conn = sqlite3.connect('inventory.db')
        logging.info("Connected to database.")

        logging.info('Creating Vendor Summary Table...')
        summary_df = create_vendor_summary(conn)
        logging.info("\n%s", summary_df.head())

        logging.info('Cleaning Data...')
        clean_df = clean_data(summary_df)
        logging.info("\n%s", clean_df.head())

        logging.info('Ingesting data....')
        ingest_db(clean_df, 'vendor_sales_summary', conn)
        logging.info('Completed successfully.')

    except Exception as e:
        logging.error("Fatal error: %s", e)
    finally:
        conn.close()
        logging.info("Database connection closed.")
