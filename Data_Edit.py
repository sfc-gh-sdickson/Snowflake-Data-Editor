import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSQLException

# Custom CSS for styling
st.markdown("""
    <style>
    /* Set page to extra wide */
    .main .block-container {
        max-width: 95%;
        padding: 1rem;
    }
    /* Reduce element sizes by 1/3 and set square corners */
    .stButton>button {
        font-size: 10px !important;
        padding: 4px 8px !important;
        border-radius: 0 !important;
        background-color: #005B96 !important; /* Snowflake blue */
        color: white !important;
        border: 1px solid #4B4B4B !important; /* Gray border */
    }
    .stButton>button:hover {
        background-color: #6A0DAD !important; /* Purple on hover */
    }
    .stSelectbox, .stTextInput {
        font-size: 10px !important;
        border-radius: 0 !important;
        background-color: #F5F5F5 !important; /* Light gray */
        border: 1px solid #4B4B4B !important;
    }
    .stDataFrame .dataframe {
        font-size: 10px !important;
        border-radius: 0 !important;
        border: 1px solid #4B4B4B !important;
    }
    .stDataFrame .dataframe th, .stDataFrame .dataframe td {
        border: 1px solid #4B4B4B !important;
        background-color: #E6F0FA !important; /* Light Snowflake blue */
    }
    .stSpinner {
        font-size: 10px !important;
    }
    /* Custom button styles */
    .custom-button {
        font-size: 10px !important;
        padding: 4px 8px !important;
        border-radius: 0 !important;
        background-color: #005B96 !important;
        color: white !important;
        border: 1px solid #4B4B4B !important;
        margin-right: 5px;
    }
    .custom-button:hover {
        background-color: #6A0DAD !important;
    }
    </style>
""", unsafe_allow_html=True)

# Set page configuration
st.set_page_config(page_title="Snowflake Table Editor", page_icon="❄️", layout="wide")

# Title
st.title("Snowflake Table Editor")

# Get active Snowflake session
session = get_active_session()

# Cache database list
@st.cache_data
def get_databases():
    try:
        dbs = session.sql("SHOW DATABASES").collect()
        return [row["name"] for row in dbs if row["name"] not in ["SNOWFLAKE"]]
    except Exception as e:
        st.error(f"Error fetching databases: {str(e)}")
        return []

# Cache schema list
@st.cache_data
def get_schemas(database):
    try:
        schemas = session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()
        return [row["name"] for row in schemas if row["name"] != "INFORMATION_SCHEMA"]
    except Exception as e:
        st.error(f"Error fetching schemas: {str(e)}")
        return []

# Cache table list
@st.cache_data
def get_tables(database, schema):
    try:
        tables = session.sql(f"SHOW TABLES IN {database}.{schema}").collect()
        return [row["name"] for row in tables]
    except Exception as e:
        st.error(f"Error fetching tables: {str(e)}")
        return []

# Cache role list
@st.cache_data
def get_roles():
    try:
        roles = session.sql("SHOW ROLES").collect()
        return [row["name"] for row in roles]
    except Exception as e:
        st.error(f"Error fetching roles: {str(e)}")
        return []

# Sidebar for selections
st.sidebar.header("Configuration")

# Role selection
roles = get_roles()
current_role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
selected_role = st.sidebar.selectbox("Select Role", roles, index=roles.index(current_role) if current_role in roles else 0)
if selected_role != current_role:
    try:
        session.sql(f"USE ROLE {selected_role}").collect()
        st.sidebar.success(f"Switched to role: {selected_role}")
        # Clear cache to refresh data with new role
        st.cache_data.clear()
    except Exception as e:
        st.sidebar.error(f"Error switching role: {str(e)}")

# Database selection
databases = get_databases()
selected_db = st.sidebar.selectbox("Select Database", databases)

# Schema selection (excluding INFORMATION_SCHEMA)
if selected_db:
    schemas = get_schemas(selected_db)
    selected_schema = st.sidebar.selectbox("Select Schema", schemas)
else:
    schemas = []
    selected_schema = None

# Table selection
if selected_db and selected_schema:
    tables = get_tables(selected_db, selected_schema)
    selected_table = st.sidebar.selectbox("Select Table", tables)
else:
    tables = []
    selected_table = None

# State management for edited DataFrame
if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame()

# Fetch and display data
if selected_db and selected_schema and selected_table:
    try:
        with st.spinner("Fetching table data..."):
            query = f"SELECT * FROM {selected_db}.{selected_schema}.{selected_table}"
            st.session_state.df = session.sql(query).to_pandas()

        # Editable table
        st.subheader(f"Editing: {selected_db}.{selected_schema}.{selected_table}")
        edited_df = st.data_editor(
            st.session_state.df,
            num_rows="dynamic",  # Allows adding/deleting rows
            use_container_width=True,
            key=f"editor_{selected_db}_{selected_schema}_{selected_table}"
        )

        # Buttons for saving changes
        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("Save Changes", key="save_button"):
                try:
                    with st.spinner("Saving changes..."):
                        # Truncate table and insert updated data
                        session.sql(f"TRUNCATE TABLE {selected_db}.{selected_schema}.{selected_table}").collect()
                        session.write_pandas(
                            edited_df,
                            database=selected_db,
                            schema=selected_schema,
                            table_name=selected_table,
                            auto_create_table=False
                        )
                        st.success("Changes saved successfully!")
                        st.session_state.df = edited_df  # Update state
                except SnowparkSQLException as e:
                    st.error(f"Error saving changes: {str(e)}")
        with col2:
            if st.button("Reset", key="reset_button"):
                st.session_state.df = session.sql(query).to_pandas()
                st.rerun()

    except SnowparkSQLException as e:
        st.error(f"Error fetching table data: {str(e)}")
else:
    st.info("Please select a database, schema, and table to begin.")

# Footer
st.markdown("""
---
### About
- Select a database, schema (excluding INFORMATION_SCHEMA), and table.
- Edit table data inline, add/delete rows, and save changes.
- Switch roles to adjust permissions.
- Styled with Snowflake blue, gray, and purple; square corners; 1/3 smaller elements.
""")
