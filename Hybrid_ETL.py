import pandas as pd
import pyodbc
import threading
import queue
import time
import getpass
from collections import deque
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


class HybridJoinThreaded:
    def __init__(self, hash_slots=10000, queue_size=5000, disk_partition_size=500):
        self.hash_slots = hash_slots
        self.queue_size = queue_size
        self.disk_partition_size = disk_partition_size
        
        self.hash_table = {}
        self.processing_queue = deque(maxlen=queue_size)
        self.result = []
        
        self.stream_buffer = queue.Queue(maxsize=10000)
        self.producer_finished = threading.Event()
        self.lock = threading.Lock()
        
        self.stats = {
            'processed': 0,
            'joined': 0,
            'dropped': 0,
            'hash_hits': 0,
            'queue_hits': 0
        }
    
    def load_master_data_to_disk(self, customer_df, product_df):
        print("Loading Master Data to Disk Buffer...")
        
        self.customer_dict = {}
        for _, cust_row in customer_df.iterrows():
            self.customer_dict[cust_row['Customer_ID']] = cust_row.to_dict()
        
        self.product_dict = {}
        for _, prod_row in product_df.iterrows():
            self.product_dict[prod_row['Product_ID']] = prod_row.to_dict()
        
        print(f"Loaded {len(self.customer_dict)} customers and {len(self.product_dict)} products")
        return self
    
    def hash_function(self, customer_id, product_id):
        combined_key = f"{customer_id}_{product_id}"
        return hash(combined_key) % self.hash_slots
    
    def producer_thread(self, transactional_df):
        print("\nTHREAD 1 (PRODUCER): Started - Feeding stream buffer...")
        
        batch_size = 1000
        total_records = len(transactional_df)
        
        for start_idx in range(0, total_records, batch_size):
            end_idx = min(start_idx + batch_size, total_records)
            batch = transactional_df.iloc[start_idx:end_idx]
            
            for _, row in batch.iterrows():
                stream_tuple = row.to_dict()
                
                self.stream_buffer.put(stream_tuple)
                
                with self.lock:
                    self.stats['processed'] += 1
            
            time.sleep(0.001)
            
            if (start_idx // batch_size) % 50 == 0:
                print(f"   Producer: Fed {end_idx:,}/{total_records:,} records into stream buffer")
        
        self.producer_finished.set()
        print("\nTHREAD 1 (PRODUCER): Finished - All data fed into stream buffer")
    
    def consumer_thread(self):
        print("\nTHREAD 2 (CONSUMER): Started - Running HYBRIDJOIN algorithm...")
        
        processed_count = 0
        
        while True:
            try:
                stream_tuple = self.stream_buffer.get(timeout=0.1)
                
                self.process_stream_tuple(stream_tuple)
                processed_count += 1
                
                if processed_count % 1000 == 0:
                    self.load_disk_partition()
                    
                    if processed_count % 10000 == 0:
                        with self.lock:
                            print(f"   Consumer: Processed {processed_count:,} | "
                                  f"Joined: {self.stats['joined']:,} | "
                                  f"Hash: {len(self.hash_table):,} slots | "
                                  f"Queue: {len(self.processing_queue):,}")
                
                self.stream_buffer.task_done()
                
            except queue.Empty:
                if self.producer_finished.is_set():
                    self.load_disk_partition()
                    break
        
        print("\nTHREAD 2 (CONSUMER): Finished - HYBRIDJOIN complete")
    
    def process_stream_tuple(self, stream_tuple):
        customer_id = stream_tuple['Customer_ID']
        product_id = stream_tuple['Product_ID']
        hash_key = self.hash_function(customer_id, product_id)
        print(customer_id,product_id,hash_key)
        with self.lock:
            if hash_key in self.hash_table:
                master_record = self.hash_table[hash_key]
                joined_record = self.perform_join(stream_tuple, master_record)
                if joined_record:
                    self.result.append(joined_record)
                    self.stats['joined'] += 1
                    self.stats['hash_hits'] += 1
            else:
                self.processing_queue.append(stream_tuple)
    
    def load_disk_partition(self):
        if not self.processing_queue:
            return
        
        with self.lock:
            tuples_to_process = list(self.processing_queue)[:self.disk_partition_size]
            self.processing_queue.clear()
        
        for stream_tuple in tuples_to_process:
            customer_id = stream_tuple['Customer_ID']
            product_id = stream_tuple['Product_ID']
            
            if customer_id in self.customer_dict and product_id in self.product_dict:
                customer_data = self.customer_dict[customer_id]
                product_data = self.product_dict[product_id]
                
                master_record = {**customer_data, **product_data}
                hash_key = self.hash_function(customer_id, product_id)
                
                with self.lock:
                    self.hash_table[hash_key] = master_record
                    
                    joined_record = self.perform_join(stream_tuple, master_record)
                    if joined_record:
                        self.result.append(joined_record)
                        self.stats['joined'] += 1
                        self.stats['queue_hits'] += 1
            else:
                with self.lock:
                    self.stats['dropped'] += 1
    
    def perform_join(self, stream_tuple, master_record):
        try:
            quantity = stream_tuple['quantity']
            price = master_record.get('price$', 0)
            total_revenue = quantity * price
            
            joined = {
                'orderID': stream_tuple['orderID'],
                'Customer_ID': stream_tuple['Customer_ID'],
                'Product_ID': stream_tuple['Product_ID'],
                'quantity': quantity,
                'date': stream_tuple['date'],
                
                'Gender': master_record.get('Gender', 'Unknown'),
                'Age': master_record.get('Age', 'Unknown'),
                'Occupation': master_record.get('Occupation', 0),
                'City_Category': master_record.get('City_Category', 'Unknown'),
                'Stay_In_Current_City_Years': master_record.get('Stay_In_Current_City_Years', 0),
                'Marital_Status': master_record.get('Marital_Status', 0),
                
                'Product_Category': master_record.get('Product_Category', 'Unknown'),
                'price': price,
                'storeID': master_record.get('storeID', 0),
                'supplierID': master_record.get('supplierID', 0),
                'storeName': master_record.get('storeName', 'Unknown'),
                'supplierName': master_record.get('supplierName', 'Unknown'),
                
                'Total_Revenue': total_revenue
            }
            print(joined)
            return joined
        except Exception as e:
            print(f"Join error: {e}")
            return None
    
    def execute_join_threaded(self, transactional_df):
        print("\n" + "="*80)
        print("EXECUTING MULTI-THREADED HYBRIDJOIN ALGORITHM")
        print("="*80)
        print(f"\nStream Input: {len(transactional_df):,} transactional records")
        print(f"Disk Buffer: {len(self.customer_dict):,} customers × {len(self.product_dict):,} products")
        print(f"Configuration:")
        print(f"   • Hash Table Size: {self.hash_slots:,} slots")
        print(f"   • Queue Capacity: {self.queue_size:,} tuples")
        print(f"   • Stream Buffer Size: 10,000 tuples (thread-safe)")
        print(f"   • Disk Partition Size: {self.disk_partition_size:,} tuples/load")
        print("\n" + "─"*80)
        
        producer = threading.Thread(
            target=self.producer_thread,
            args=(transactional_df,),
            name="ProducerThread"
        )
        
        consumer = threading.Thread(
            target=self.consumer_thread,
            name="ConsumerThread"
        )
        
        start_time = time.time()
        
        producer.start()
        consumer.start()
        
        producer.join()
        consumer.join()
        
        elapsed_time = time.time() - start_time
        
        print("\n" + "─"*80)
        print(f"\nMULTI-THREADED HYBRIDJOIN COMPLETE!")
        print(f"\nFinal Statistics:")
        print(f"   • Total Records Processed: {self.stats['processed']:,}")
        print(f"   • Successfully Joined: {self.stats['joined']:,} ({(self.stats['joined']/self.stats['processed']*100):.2f}%)")
        print(f"   • Dropped (No Match): {self.stats['dropped']:,} ({(self.stats['dropped']/self.stats['processed']*100):.2f}%)")
        print(f"   • Hash Table Hits: {self.stats['hash_hits']:,}")
        print(f"   • Queue Processing Hits: {self.stats['queue_hits']:,}")
        print(f"   • Hash Table Utilization: {len(self.hash_table):,} / {self.hash_slots:,} slots ({(len(self.hash_table)/self.hash_slots*100):.2f}%)")
        print(f"   • Execution Time: {elapsed_time:.2f} seconds")
        print(f"   • Throughput: {(self.stats['processed']/elapsed_time):,.0f} records/second")
        print("="*80)
        
        return pd.DataFrame(self.result)


def get_connection_details():
    print("\n" + "="*80)
    print("SQL SERVER CONNECTION SETUP")
    print("="*80)
    
    print("\nEnter SQL Server Connection Details:")
    print("   (Press Enter for default values shown in brackets)")
    print("")
    
    server = input("Server name [localhost\\SQLEXPRESS]: ").strip()
    if not server:
        server = "localhost\\SQLEXPRESS"
    
    database = input("Database name [November2025DW]: ").strip()
    if not database:
        database = "November2025DW"
    
    print("\nAuthentication Type:")
    print("  1. Windows Authentication (Trusted Connection)")
    print("  2. SQL Server Authentication (Username/Password)")
    
    auth_choice = input("Select authentication type [1]: ").strip()
    if not auth_choice:
        auth_choice = "1"
    
    username = None
    password = None
    
    if auth_choice == "2":
        username = input("Username [sa]: ").strip()
        if not username:
            username = "sa"
        password = getpass.getpass("Password: ")
    
    return server, database, auth_choice, username, password


def connect_to_sql_server():
    server, database, auth_choice, username, password = get_connection_details()
    
    print("\nConnecting to SQL Server...")
    print(f"   Server: {server}")
    print(f"   Database: {database}")
    print(f"   Authentication: {'Windows' if auth_choice == '1' else 'SQL Server'}")
    
    drivers = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
        "SQL Server Native Client 11.0"
    ]
    
    for driver in drivers:
        try:
            if auth_choice == "1":
                conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
                if "18" in driver:
                    conn_str += "TrustServerCertificate=yes;"
            else:
                conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};"
                if "18" in driver:
                    conn_str += "TrustServerCertificate=yes;"
            
            conn = pyodbc.connect(conn_str, timeout=10)
            print(f"   Connected successfully using: {driver}")
            return conn
        except Exception as e:
            continue
    
    print("\nCould not connect to SQL Server.")
    print("\nPlease check:")
    print("  1. SQL Server Express is running")
    print("  2. Server name is correct (e.g., localhost\\SQLEXPRESS)")
    print("  3. Database 'November2025DW' exists (run create_star_schema.sql first)")
    print("  4. Credentials are correct (if using SQL Auth)")
    print("  5. ODBC Driver is installed")
    raise Exception("Database connection failed")


def load_dimensions(conn, enriched_data):
    print("\n" + "="*80)
    print("LOADING DIMENSION TABLES TO SQL SERVER")
    print("="*80)
    cursor = conn.cursor()
    
    print("\nDim_Customer:")
    customers = enriched_data[['Customer_ID', 'Gender', 'Age', 'Occupation', 
                                'City_Category', 'Stay_In_Current_City_Years', 'Marital_Status']].drop_duplicates()
    
    for idx, (_, row) in enumerate(customers.iterrows(), 1):
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dbo.Dim_Customer WHERE Customer_ID = ?)
            INSERT INTO dbo.Dim_Customer (Customer_ID, Gender, Age, Occupation, City_Category, Stay_In_Current_City_Years, Marital_Status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, row['Customer_ID'], row['Customer_ID'], row['Gender'], row['Age'], 
             row['Occupation'], row['City_Category'], row['Stay_In_Current_City_Years'], row['Marital_Status'])
        if idx % 500 == 0 or idx == len(customers):
            print(f"   Progress: {idx:,}/{len(customers):,}", end='\r')
    conn.commit()
    print(f"\n   Loaded {len(customers):,} customers")
    
    print("\nDim_Product:")
    products = enriched_data[['Product_ID', 'Product_Category', 'price']].drop_duplicates()
    
    for idx, (_, row) in enumerate(products.iterrows(), 1):
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dbo.Dim_Product WHERE Product_ID = ?)
            INSERT INTO dbo.Dim_Product (Product_ID, Product_Category, Price)
            VALUES (?, ?, ?)
        """, row['Product_ID'], row['Product_ID'], row['Product_Category'], float(row['price']))
        if idx % 500 == 0 or idx == len(products):
            print(f"   Progress: {idx:,}/{len(products):,}", end='\r')
    conn.commit()
    print(f"\n   Loaded {len(products):,} products")
    
    print("\nDim_Date:")
    enriched_data['date'] = pd.to_datetime(enriched_data['date'])
    dates = enriched_data[['date']].drop_duplicates()
    
    for idx, (_, row) in enumerate(dates.iterrows(), 1):
        date_val = row['date']
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dbo.Dim_Date WHERE Date = ?)
            INSERT INTO dbo.Dim_Date (Date, Year, Month, Day, Quarter, Day_of_Week, Day_Name, Day_Type, Month_Name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, date_val, date_val, date_val.year, date_val.month, date_val.day, 
             (date_val.month - 1) // 3 + 1, date_val.weekday(), 
             date_val.strftime('%A'), 'Weekend' if date_val.weekday() >= 5 else 'Weekday',
             date_val.strftime('%B'))
        if idx % 200 == 0 or idx == len(dates):
            print(f"   Progress: {idx:,}/{len(dates):,}", end='\r')
    conn.commit()
    print(f"\n   Loaded {len(dates):,} dates")
    
    print("\nDim_Store:")
    stores = enriched_data[['storeID', 'storeName']].drop_duplicates()
    for _, row in stores.iterrows():
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dbo.Dim_Store WHERE Store_ID = ?)
            INSERT INTO dbo.Dim_Store (Store_ID, Store_Name)
            VALUES (?, ?)
        """, int(row['storeID']), int(row['storeID']), row['storeName'])
    conn.commit()
    print(f"   Loaded {len(stores):,} stores")
    
    print("\nDim_Supplier:")
    suppliers = enriched_data[['supplierID', 'supplierName']].drop_duplicates()
    for _, row in suppliers.iterrows():
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dbo.Dim_Supplier WHERE Supplier_ID = ?)
            INSERT INTO dbo.Dim_Supplier (Supplier_ID, Supplier_Name)
            VALUES (?, ?)
        """, int(row['supplierID']), int(row['supplierID']), row['supplierName'])
    conn.commit()
    print(f"   Loaded {len(suppliers):,} suppliers")
    
    print("\n" + "="*80)
    cursor.close()


def load_fact_table(conn, enriched_data):
    print("\n" + "="*80)
    print("LOADING FACT_SALES TABLE")
    print("="*80)
    print("\nThis may take 2-5 minutes...")
    
    cursor = conn.cursor()
    enriched_data['date'] = pd.to_datetime(enriched_data['date'])
    
    batch_size = 1000
    total_rows = len(enriched_data)
    
    print(f"\nTotal Records: {total_rows:,}\n")
    
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch = enriched_data.iloc[start_idx:end_idx]
        
        for _, row in batch.iterrows():
            cursor.execute("""
                INSERT INTO dbo.Fact_Sales (Order_ID, Customer_SK, Product_SK, Date_SK, Store_SK, Supplier_SK, Quantity, Total_Revenue)
                SELECT 
                    ?,
                    c.Customer_SK,
                    p.Product_SK,
                    d.Date_SK,
                    s.Store_SK,
                    sup.Supplier_SK,
                    ?,
                    ?
                FROM dbo.Dim_Customer c
                CROSS JOIN dbo.Dim_Product p
                CROSS JOIN dbo.Dim_Date d
                CROSS JOIN dbo.Dim_Store s
                CROSS JOIN dbo.Dim_Supplier sup
                WHERE c.Customer_ID = ?
                  AND p.Product_ID = ?
                  AND d.Date = ?
                  AND s.Store_ID = ?
                  AND sup.Supplier_ID = ?
            """, int(row['orderID']), int(row['quantity']), float(row['Total_Revenue']),
                 int(row['Customer_ID']), row['Product_ID'], row['date'],
                 int(row['storeID']), int(row['supplierID']))
        
        conn.commit()
        progress = (end_idx / total_rows) * 100
        print(f"Progress: {end_idx:,}/{total_rows:,} [{progress:5.1f}%]", end='\r')
    
    print(f"\n\nSuccessfully loaded {total_rows:,} transactions")
    print("="*80)
    cursor.close()


def verify_data(conn):
    print("\n" + "="*80)
    print("DATA VERIFICATION")
    print("="*80)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM dbo.Dim_Customer")
    cust_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dbo.Dim_Product")
    prod_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dbo.Dim_Date")
    date_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dbo.Dim_Store")
    store_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dbo.Dim_Supplier")
    supp_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dbo.Fact_Sales")
    fact_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT SUM(Total_Revenue) FROM dbo.Fact_Sales")
    total_revenue = cursor.fetchone()[0]
    
    print("\nDimension Tables:")
    print(f"   Dim_Customer:  {cust_count:>7,} records")
    print(f"   Dim_Product:   {prod_count:>7,} records")
    print(f"   Dim_Date:      {date_count:>7,} records")
    print(f"   Dim_Store:     {store_count:>7,} records")
    print(f"   Dim_Supplier:  {supp_count:>7,} records")
    
    print(f"\nFact Table:")
    print(f"   Fact_Sales:    {fact_count:>7,} transactions")
    
    if total_revenue:
        print(f"\nBusiness Metrics:")
        print(f"   Total Revenue:    ${total_revenue:>12,.2f}")
        print(f"   Avg Transaction:  ${(total_revenue/fact_count):>12,.2f}")
    
    print("\n" + "="*80)
    cursor.close()


def main():
    print("\n" + "="*80)
    print(" "*15 + "WALMART DATA WAREHOUSE")
    print(" "*10 + "MULTI-THREADED HYBRIDJOIN ETL PIPELINE")
    print("="*80)
    
    print("\nLoading CSV Files...")
    customer_df = pd.read_csv('customer_master_data.csv', index_col=0)
    product_df = pd.read_csv('product_master_data.csv', index_col=0)
    transactional_df = pd.read_csv('transactional_data.csv', index_col=0)
    
    print(f"   Customers: {len(customer_df):,}")
    print(f"   Products: {len(product_df):,}")
    print(f"   Transactions: {len(transactional_df):,}")
    
    hybrid_join = HybridJoinThreaded(
        hash_slots=10000,
        queue_size=5000,
        disk_partition_size=500
    )
    
    hybrid_join.load_master_data_to_disk(customer_df, product_df)
    enriched_data = hybrid_join.execute_join_threaded(transactional_df)
    
    print(f"\nEnriched Data Shape: {enriched_data.shape}")
    print(f"Sample Data (first 5 rows):")
    print("─"*80)
    sample_cols = ['orderID', 'Customer_ID', 'Product_ID', 'Gender', 'Age', 
                   'Product_Category', 'price', 'quantity', 'Total_Revenue', 'storeName']
    print(enriched_data[sample_cols].head().to_string(index=False))
    print("─"*80)
    
    try:
        conn = connect_to_sql_server()
        
        print("\n" + "="*80)
        print("STEP 3: LOADING TO SQL SERVER DATA WAREHOUSE")
        print("="*80)
        
        load_dimensions(conn, enriched_data)
        load_fact_table(conn, enriched_data)
        verify_data(conn)
        
        conn.close()
        
        print("\n" + "="*80)
        print(" "*25 + "ETL COMPLETE!")
        print("="*80)
        print("\nSUCCESS! Your Data Warehouse is Ready!")
        print("\nNext Steps:")
        print("   1. Open SQL Server Management Studio (SSMS)")
        print("   2. Connect to your SQL Express instance")
        print("   3. Execute: 3_olap_queries.sql")
        print("   4. Explore 20 business intelligence queries")
        print("\n" + "="*80)
        
    except Exception as e:
        print(f"\nError connecting to SQL Server: {e}")
        print("\nMake sure:")
        print("  1. SQL Server Express is running")
        print("  2. Run create_star_schema.sql first to create database and tables")
        print("  3. Install pyodbc: pip install pyodbc")
        print("\nData is enriched and ready - stored in enriched_data DataFrame")
        print(f"   {len(enriched_data):,} records available for analysis")
        print("\n" + "="*80)


if __name__ == "__main__":
    main()
