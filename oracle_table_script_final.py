import streamlit as st
import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from io import BytesIO
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
from datetime import datetime
import os
import pyodbc

# ---------- CONFIGURATION ----------
DEFAULT_SEARCH_DOMAIN = "docs.oracle.com/en/cloud/saas/"
USER_AGENT = {"User-Agent": "Mozilla/5.0"}

# Default table prefix
DEFAULT_TABLE_PREFIX = "ST_FN_"

# *** ENTER YOUR CREDENTIALS HERE ***
GOOGLE_API_KEY = st.secrets.get("GOOGLE_API_KEY", "YOUR_API_KEY_HERE")
GOOGLE_CSE_ID = st.secrets.get("GOOGLE_CSE_ID", "YOUR_CSE_ID_HERE")

# SQL Server Configuration - Using Streamlit secrets for security
SQL_SERVER = st.secrets.get("SQL_SERVER", "43.231.235.199")
SQL_USERNAME = st.secrets.get("SQL_USERNAME", "sa")
SQL_PASSWORD = st.secrets.get("SQL_PASSWORD", "love")
SQL_DATABASE = st.secrets.get("SQL_DATABASE", "master")  # Default database

# Counter file to track daily usage
COUNTER_FILE = "api_usage_counter.json"

# Initialize session state
if 'results_ready' not in st.session_state:
    st.session_state.results_ready = False
if 'conv_df' not in st.session_state:
    st.session_state.conv_df = None
if 'sql_script' not in st.session_state:
    st.session_state.sql_script = None
if 'table_name' not in st.session_state:
    st.session_state.table_name = None
if 'doc_url' not in st.session_state:
    st.session_state.doc_url = None
if 'show_db_selection' not in st.session_state:
    st.session_state.show_db_selection = False
if 'selected_database' not in st.session_state:
    st.session_state.selected_database = None
# Prefix persistence: default to DEFAULT_TABLE_PREFIX on first run
if 'table_prefix' not in st.session_state:
    st.session_state.table_prefix = DEFAULT_TABLE_PREFIX

# ---------- SQL Server Functions ----------

def test_sql_connection():
    """Test SQL Server connection"""
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD}"
        )
        conn = pyodbc.connect(conn_str, timeout=5)
        conn.close()
        return True, "Connection successful!"
    except Exception as e:
        return False, str(e)


def execute_sql_script(sql_script, database_name=None):
    """Execute SQL script on SQL Server"""
    try:
        # Use specified database or default
        db = database_name if database_name else SQL_DATABASE
        
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={db};"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD}"
        )
        
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()
        
        # Execute the SQL script
        cursor.execute(sql_script)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return True, "Table created successfully!"
    except pyodbc.Error as e:
        error_message = str(e)
        # Check if it's a table already exists error
        if "already an object" in error_message.lower() or "already exists" in error_message.lower():
            return False, "TABLE_EXISTS"
        else:
            return False, error_message
    except Exception as e:
        return False, str(e)


def check_table_exists(table_name, database_name=None, prefix=None):
    """Check if table already exists in SQL Server"""
    try:
        # Use specified database or default
        db = database_name if database_name else SQL_DATABASE
        prefix_to_use = prefix if prefix is not None else st.session_state.get('table_prefix', DEFAULT_TABLE_PREFIX)
        
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={db};"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD}"
        )
        
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()
        
        # Check if table exists. Use upper-case to be consistent.
        check_query = f"""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = '{(prefix_to_use + table_name).upper()}'
        """
        
        cursor.execute(check_query)
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return result[0] > 0
    except Exception as e:
        st.warning(f"Could not check if table exists: {e}")
        return False


def get_databases():
    """Get list of databases from SQL Server"""
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE=master;"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD}"
        )
        
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb') ORDER BY name")
        databases = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return databases
    except Exception as e:
        st.error(f"Error fetching databases: {e}")
        return []


# ---------- API Usage Tracking ----------

def load_usage_counter():
    """Load the API usage counter from file"""
    if os.path.exists(COUNTER_FILE):
        with open(COUNTER_FILE, 'r') as f:
            data = json.load(f)
            return data.get('count', 0), data.get('date', '')
    return 0, ''


def save_usage_counter(count):
    """Save the API usage counter to file"""
    today = datetime.now().strftime('%Y-%m-%d')
    with open(COUNTER_FILE, 'w') as f:
        json.dump({'count': count, 'date': today}, f)


def check_and_update_counter():
    """Check if we can make API call and update counter"""
    count, last_date = load_usage_counter()
    today = datetime.now().strftime('%Y-%m-%d')

    # Reset counter if it's a new day
    if last_date != today:
        count = 0

    # Check if limit reached
    if count >= 100:
        st.error("🚫 Daily API limit reached (100/100 searches used).")
        st.warning("⏰ Please come back tomorrow. The counter will reset at midnight.")
        st.info(f"📅 Current date: {today}")
        return False

    # Increment counter
    count += 1
    save_usage_counter(count)
    st.info(f"📊 API Usage: {count}/100 searches used today")
    return True


# ---------- Utility Functions ----------

def get_oracle_doc_url_scrape(table_name):
    """Fallback Google HTML scraping"""
    # Try exact table name first, only HTML pages
    q = f'"{table_name}" site:docs.oracle.com/en/cloud/saas/ filetype:html'
    try:
        res = requests.get("https://www.google.com/search", params={"q": q}, headers=USER_AGENT, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")

        # Look for links containing the table name
        candidates = []
        for a in soup.select("a"):
            href = a.get("href", "")
            if "docs.oracle.com/en/cloud/saas/" in href and ".html" in href:
                m = re.search(r"https://docs\.oracle\.com[^&]+\.html", href)
                if m:
                    url = m.group(0)
                    # Skip Excel, PDF, and other non-HTML files
                    if any(ext in url.lower() for ext in ['.xlsx', '.pdf', '.zip', '.xml']):
                        continue
                    # Skip index and overview pages
                    if any(skip in url.lower() for skip in ['index.html', 'toc.html', 'preface']):
                        continue

                    # Calculate match score
                    table_name_clean = table_name.lower().replace("_", "")
                    url_lower = url.lower()

                    # Exact match in URL gets highest score
                    if table_name_clean in url_lower.replace("-", "").replace("_", ""):
                        return url

                    candidates.append(url)

        # Return first candidate if found
        if candidates:
            return candidates[0]

    except Exception as e:
        st.warning(f"HTML scraping failed: {e}")
    return None


def get_oracle_doc_url_api(table_name, api_key, cse_id):
    """Google Custom Search API version"""
    try:
        service = build("customsearch", "v1", developerKey=api_key)
        # Search for exact phrase, only HTML files
        query = f'"{table_name}" site:docs.oracle.com/en/cloud/saas/ filetype:html'
        res = service.cse().list(q=query, cx=cse_id, num=10).execute()

        # Filter and prioritize results
        candidates = []
        for item in res.get("items", []):
            link = item.get("link", "")

            # Must be from Oracle docs and HTML
            if "docs.oracle.com/en/cloud/saas/" not in link or not link.endswith(".html"):
                continue

            # Skip Excel, PDF, and other non-HTML files
            if any(ext in link.lower() for ext in ['.xlsx', '.pdf', '.zip', '.xml']):
                continue

            # Skip index and overview pages
            if any(skip in link.lower() for skip in ['index.html', 'toc.html', 'preface', 'overview']):
                continue

            # Check if table name appears in URL (clean comparison)
            table_name_clean = table_name.lower().replace("_", "")
            url_clean = link.lower().replace("-", "").replace("_", "")

            # Exact match gets returned immediately
            if table_name_clean in url_clean:
                return link

            candidates.append(link)

        # Return first valid candidate
        if candidates:
            return candidates[0]

        return None

    except HttpError as e:
        if "quota" in str(e).lower():
            st.error("🚫 Google API quota exceeded. The daily limit has been reached.")
            st.warning("⏰ Please come back tomorrow or use the HTML scraping method (toggle off the API option).")
        raise e
    return None


def scrape_columns(url):
    """Extract the columns table from the Oracle doc page"""
    try:
        res = requests.get(url, headers=USER_AGENT, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        # Debug: Show all tables found
        tables = soup.find_all("table")
        st.info(f"Found {len(tables)} table(s) on the page")

        # Try multiple strategies to find the columns table
        for idx, t in enumerate(tables):
            headers = [th.get_text().strip().upper() for th in t.find_all("th")]
            st.write(f"Table {idx + 1} headers: {headers}")

            # Look specifically for the Columns table (has Name, Datatype, Length, etc.)
            if "NAME" in headers and "DATATYPE" in headers:
                try:
                    df = pd.read_html(str(t))[0]
                    st.success(f"✅ Found columns table (Table {idx + 1})")
                    st.write(f"Shape: {df.shape}, Columns: {list(df.columns)}")
                    return df
                except Exception as e:
                    st.warning(f"Could not parse table {idx + 1}: {e}")
                    continue

        st.error("❌ No suitable columns table found")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error scraping columns: {e}")
        return pd.DataFrame()


def convert_datatypes(df):
    """Convert Oracle data types → SQL Server types"""
    st.write("🔍 **Original DataFrame:**")
    st.dataframe(df.head(10))

    # Normalize column names
    df.columns = [c.strip().upper().replace(" ", "_").replace("-", "_") for c in df.columns]
    st.write(f"📋 Normalized columns: {list(df.columns)}")

    # The Oracle docs have these exact column names
    colname_col = "NAME"
    dtype_col = "DATATYPE"
    length_col = "LENGTH"
    precision_col = "PRECISION"
    notnull_col = "NOT_NULL"
    comments_col = "COMMENTS"

    # Verify columns exist
    if colname_col not in df.columns or dtype_col not in df.columns:
        st.error(f"❌ Required columns missing. Found: {list(df.columns)}")
        return pd.DataFrame()

    st.write(f"🎯 Using columns - Name: {colname_col}, Type: {dtype_col}, Length: {length_col}")

    converted = []
    for _, row in df.iterrows():
        colname = str(row.get(colname_col, "")).strip()
        dtype = str(row.get(dtype_col, "")).strip().upper()
        length = str(row.get(length_col, "")).strip() if length_col in df.columns else ""
        precision = str(row.get(precision_col, "")).strip() if precision_col in df.columns else ""
        notnull = str(row.get(notnull_col, "")).strip() if notnull_col in df.columns else ""
        comments = str(row.get(comments_col, "")).strip() if comments_col in df.columns else ""

        if not colname or colname == "NAN":
            continue

        sqltype = dtype

        # VARCHAR conversion with exact rules
        if "VARCHAR" in dtype:
            # Try to get length from the Length column first
            n = None
            if length and length.isdigit():
                n = int(length)
            else:
                # Try to extract from datatype itself (e.g., VARCHAR2(64))
                m = re.search(r"\((\d+)\)", dtype)
                if m:
                    n = int(m.group(1))

            if n == 1:
                sqltype = "VARCHAR(1)"
            elif n and 2 <= n <= 240:
                sqltype = "NVARCHAR(240)"
            elif n and n > 240:
                sqltype = f"NVARCHAR({n})"
            else:
                sqltype = "NVARCHAR(240)"

        # NUMBER conversion with exact rules
        elif "NUMBER" in dtype:
            # Try to get precision
            prec = None
            if precision and precision.isdigit():
                prec = int(precision)
            else:
                # Try to extract from datatype itself (e.g., NUMBER(10))
                m = re.search(r"\((\d+)", dtype)
                if m:
                    prec = int(m.group(1))

            if prec and prec > 4:
                sqltype = "BIGINT"
            else:
                sqltype = "FLOAT"

        # DATE conversion
        elif "DATE" in dtype:
            sqltype = "DATETIME"

        # TIMESTAMP conversion
        elif "TIMESTAMP" in dtype:
            sqltype = "DATETIME"

        # Keep original if no match
        else:
            sqltype = dtype

        converted.append({
            "COLUMN_NAME": colname,
            "ORACLE_TYPE": dtype,
            "LENGTH": length if length else "",
            "PRECISION": precision if precision else "",
            "NOT_NULL": notnull if notnull else "",
            "SQL_SERVER_TYPE": sqltype,
            "COMMENTS": comments if comments else ""
        })

    result_df = pd.DataFrame(converted)
    st.write("✅ **Converted Data Types:**")
    st.dataframe(result_df)
    return result_df


def generate_sql(table_name, df, prefix=None):
    """Build CREATE TABLE SQL using provided prefix (or session prefix)"""
    if df.empty:
        return "-- No columns to generate"

    prefix_to_use = prefix if prefix is not None else st.session_state.get('table_prefix', DEFAULT_TABLE_PREFIX)
    lines = [f"CREATE TABLE {prefix_to_use}{table_name.upper()} ("]
    for _, r in df.iterrows():
        lines.append(f"    {r['COLUMN_NAME']} {r['SQL_SERVER_TYPE']},")
    if len(lines) > 1:
        lines[-1] = lines[-1].rstrip(",")
    lines.append(");")
    return "\n".join(lines)


# ---------- Streamlit UI ----------

st.title("🔄 Oracle → SQL Server Table Script Generator")

# Display API credentials status
credentials_configured = (GOOGLE_API_KEY != "YOUR_API_KEY_HERE" and
                          GOOGLE_CSE_ID != "YOUR_CSE_ID_HERE" and
                          GOOGLE_API_KEY.strip() != "" and
                          GOOGLE_CSE_ID.strip() != "")

if not credentials_configured:
    st.warning("⚠️ Google API credentials not configured in code. You can still use HTML scraping (leave toggle OFF).")
else:
    st.success("✅ Google API credentials configured")

# SQL Server Connection Status
with st.expander("🔌 SQL Server Connection", expanded=False):
    if st.button("Test SQL Server Connection"):
        with st.spinner("Testing connection..."):
            success, message = test_sql_connection()
            if success:
                st.success(f"✅ {message}")
                st.info(f"Connected to: {SQL_SERVER}")
            else:
                st.error(f"❌ Connection failed: {message}")

# Prefix input area
st.write("---")
st.subheader("⚙️ Table Prefix (persistent during session)")
# Show current prefix in a text input so user can change it. Use session_state to remember last-used.
prefix_input = st.text_input("Enter table prefix (example: ST_FN_ or ST_OM_):", value=st.session_state.table_prefix, max_chars=20, key="prefix_input")

# Validate prefix_input: ensure it ends with underscore for consistency
if prefix_input and not prefix_input.endswith("_"):
    st.info("Tip: it's common to end the prefix with an underscore (e.g., ST_FN_). You can omit it if you prefer.")
# Note: we don't force an underscore; we just show helpful tip.

table_name_input = st.text_input("Enter Oracle Table Name (e.g. AP_INVOICES_ALL):").strip()

use_google_api = st.toggle("Use Google Custom Search API (Free 100 queries/day)")

# Show current usage if API is enabled
if use_google_api:
    count, last_date = load_usage_counter()
    today = datetime.now().strftime('%Y-%m-%d')
    if last_date == today:
        st.info(f"📊 Current usage today: {count}/100 searches")
    else:
        st.info(f"📊 Current usage today: 0/100 searches (counter reset)")

# Add a "Start New Search" button to reset
if st.session_state.results_ready:
    if st.button("🔄 Start New Search"):
        st.session_state.results_ready = False
        st.session_state.conv_df = None
        st.session_state.sql_script = None
        st.session_state.table_name = None
        st.session_state.doc_url = None
        # keep the prefix in session_state so it remains as default for the next search
        st.rerun()

if st.button("Generate") and not st.session_state.results_ready:
    if not table_name_input:
        st.error("Please enter a table name.")
        st.stop()

    # Update and persist prefix chosen by user BEFORE generation
    chosen_prefix = prefix_input if prefix_input else st.session_state.table_prefix
    # Save the chosen prefix in session_state so next searches use it
    st.session_state.table_prefix = chosen_prefix

    with st.spinner("🔍 Searching Oracle documentation..."):
        url = None

        # Use Google API if enabled
        if use_google_api:
            # Check credentials
            if not credentials_configured:
                st.error("❌ Please configure your Google API credentials in the code first!")
                st.info("Edit lines 19-20 in the script to add your API key and CSE ID.")
                st.stop()

            # Check usage limit
            if not check_and_update_counter():
                st.stop()

            # Make API call
            try:
                url = get_oracle_doc_url_api(table_name_input, GOOGLE_API_KEY, GOOGLE_CSE_ID)
            except Exception as e:
                st.warning(f"Google API search failed: {e}. Falling back to HTML scraping.")

        # Use HTML scraping if API not used or failed
        if not url:
            st.info("🔄 Using HTML scraping method...")
            url = get_oracle_doc_url_scrape(table_name_input)

        if not url:
            st.error("❌ No valid Oracle documentation link found.")
            st.info(f"💡 Try searching manually at: https://docs.oracle.com/en/cloud/saas/")
            st.info(f"🔍 Search term used: {table_name_input}")
            st.stop()

        # Check if table name appears in URL (remove underscores for comparison)
        table_name_clean = table_name_input.replace("_", "").lower()
        url_clean = url.replace("-", "").replace("_", "").lower()

        if table_name_clean in url_clean:
            st.success(f"✅ Found documentation: [{url}]({url})")
        else:
            st.warning(f"⚠️ Found documentation (URL doesn't contain exact table name): [{url}]({url})")
            st.info(f"🔍 Searched for: {table_name_input}")
            st.info("This might still be correct - Oracle URLs often have different formatting.")

        st.session_state.doc_url = url

    with st.spinner("📄 Extracting column details..."):
        df = scrape_columns(url)
        if df.empty:
            st.error("❌ Could not find or parse the columns table on the page.")
            st.info("Please check the URL manually to verify the table structure.")
            st.stop()

        conv = convert_datatypes(df)
        if conv.empty:
            st.error("❌ No valid columns were converted.")
            st.stop()

        # Generate SQL using chosen prefix
        sql_script = generate_sql(table_name_input, conv, prefix=st.session_state.table_prefix)

        # Store in session state
        st.session_state.conv_df = conv
        st.session_state.sql_script = sql_script
        st.session_state.table_name = table_name_input
        st.session_state.results_ready = True

        st.success(f"✅ Successfully extracted {len(conv)} columns!")

# Display results if they exist in session state
if st.session_state.results_ready and st.session_state.conv_df is not None:
    prefix_display = st.session_state.get('table_prefix', DEFAULT_TABLE_PREFIX)
    st.success(f"✅ Results for table: **{prefix_display}{st.session_state.table_name}**")

    if st.session_state.doc_url:
        st.info(f"📄 Source: {st.session_state.doc_url}")

    # Create Excel file
    buf = BytesIO()
    st.session_state.conv_df.to_excel(buf, index=False, engine='openpyxl')
    buf.seek(0)

    # Download buttons and Create Table button
    col1, col2, col3 = st.columns(3)

    with col1:
        st.download_button(
            label="📥 Download Excel",
            data=buf,
            file_name=f"{st.session_state.table_name}_columns.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_excel"
        )

    with col2:
        st.download_button(
            label="📄 Download SQL Script",
            data=st.session_state.sql_script.encode("utf-8"),
            file_name=f"{st.session_state.table_name}_create.sql",
            mime="text/plain",
            key="download_sql"
        )

    with col3:
        # Create table button with proper state management
        if st.button("🗄️ Create Table in SQL Server", key="create_table_btn"):
            st.session_state.show_db_selection = True
    
    # Show database selection outside of columns to prevent rerun issues
    if st.session_state.show_db_selection:
        st.write("---")
        st.subheader("📊 Create Table in SQL Server")
        
        # Get list of databases
        with st.spinner("Loading databases..."):
            databases = get_databases()
        
        prefix_in_use = st.session_state.get('table_prefix', DEFAULT_TABLE_PREFIX)
        if databases:
            selected_db = st.selectbox(
                "Select Database:",
                options=databases,
                key="database_selector"
            )
            
            col_create1, col_create2 = st.columns([1, 4])
            
            with col_create1:
                if st.button("✅ Create Table", key="confirm_create_btn"):
                    # First check if table already exists (use prefix_in_use)
                    table_exists = check_table_exists(st.session_state.table_name, selected_db, prefix=prefix_in_use)
                    
                    if table_exists:
                        st.error(f"❌ Table **{prefix_in_use}{st.session_state.table_name}** already exists in database **{selected_db}**!")
                        st.warning("⚠️ Please search for another table or drop the existing table first.")
                        st.info("💡 Tip: Click '🔄 Start New Search' above to search for a different table.")
                    else:
                        with st.spinner(f"Creating table in {selected_db}..."):
                            success, message = execute_sql_script(st.session_state.sql_script, selected_db)
                            
                            if success:
                                st.success(f"✅ {message}")
                                st.info(f"Table **{prefix_in_use}{st.session_state.table_name}** created in database: **{selected_db}**")
                                st.session_state.show_db_selection = False
                            elif message == "TABLE_EXISTS":
                                st.error(f"❌ Table **{prefix_in_use}{st.session_state.table_name}** already exists in database **{selected_db}**!")
                                st.warning("⚠️ Please search for another table or drop the existing table first.")
                                st.info("💡 Tip: Click '🔄 Start New Search' above to search for a different table.")
                            else:
                                st.error(f"❌ Failed to create table: {message}")
            
            with col_create2:
                if st.button("❌ Cancel", key="cancel_btn"):
                    st.session_state.show_db_selection = False
                    st.rerun()
        else:
            st.warning("⚠️ No user databases found. Using default database 'master'...")
            
            col_create1, col_create2 = st.columns([1, 4])
            
            with col_create1:
                if st.button("✅ Create Table", key="confirm_create_default_btn"):
                    # First check if table already exists
                    table_exists = check_table_exists(st.session_state.table_name, "master", prefix=prefix_in_use)
                    
                    if table_exists:
                        st.error(f"❌ Table **{prefix_in_use}{st.session_state.table_name}** already exists in database **master**!")
                        st.warning("⚠️ Please search for another table or drop the existing table first.")
                        st.info("💡 Tip: Click '🔄 Start New Search' above to search for a different table.")
                    else:
                        with st.spinner("Creating table in master database..."):
                            success, message = execute_sql_script(st.session_state.sql_script, "master")
                            
                            if success:
                                st.success(f"✅ {message}")
                                st.info(f"Table **{prefix_in_use}{st.session_state.table_name}** created in database: **master**")
                                st.session_state.show_db_selection = False
                            elif message == "TABLE_EXISTS":
                                st.error(f"❌ Table **{prefix_in_use}{st.session_state.table_name}** already exists in database **master**!")
                                st.warning("⚠️ Please search for another table or drop the existing table first.")
                                st.info("💡 Tip: Click '🔄 Start New Search' above to search for a different table.")
                            else:
                                st.error(f"❌ Failed to create table: {message}")
            
            with col_create2:
                if st.button("❌ Cancel", key="cancel_default_btn"):
                    st.session_state.show_db_selection = False
                    st.rerun()
