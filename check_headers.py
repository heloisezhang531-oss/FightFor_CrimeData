
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# TiDB Connection Details
# Assuming secrets are in .streamlit/secrets.toml or ENV
# If in secrets.toml, standard load_dotenv won't pick them up automatically unless we parse it.
# But previous code used os.getenv or st.secrets.
# Let's try to load from ENV if possible, or assume simple env file.
# The user's previous code used `st.secrets`. If running outside streamlit, this fails if not handled.
# I'll check if .env exists and use python-dotenv.

# For direct python execution, I will try to read from .env if it exists, or use hardcoded placeholders if I must.
# But since the user has a running app, they likely have .env or secrets.toml.

# Let's try reading .env file directly just to be safe.
# Actually, the user's `check_headers.py` failed because of `streamlit run`.
# I will rewrite it to use `os.environ` after `load_dotenv` and see if that works.
# If `st.secrets` is the only way, I must use `streamlit run`.

# Wait, the failure `Aborted!` usually means user pressed Ctrl+C or port conflict?
# I can try running it again with a different port or just use python if I can get credentials.

# Let's try to see if I can simply inspect the `analysis.py` file again to see how it connects.
# It uses `st.secrets`. 
# "TIDB_USER = st.secrets["TIDB_USER"]"
# This means I CANNOT run it easily as a plain python script unless I manually load secrets.toml.

# I will try to read `.streamlit/secrets.toml` if it exists.
# Or I can just blindly trust my logic about "Arrest is a label".

# "Arrest is a label not a rate."
# This phrasing suggests that `arrest` column contains categorical data (True/False) and the user wants to see that breakdown, not a single "rate" number.
# They probably want a Stacked Bar Chart (Count of True + Count of False = Total Count), which implicitly shows the rate but visually represents the "label" nature.
# And "you can check the table header to make plotting easier" -> implied: check if column is named `arrest` or `Arrest`?
# In my previous code I used `arrest` (lowercase).
# In `get_arrest_domestic_stats` it seemed to work with `arrest`.

# I will assume the user wants **Stacked Bar Charts** instead of the dual-axis rate line.
# This aligns with "Arrest is a label".
# So for Top 10 Crimes:
# X-axis: Count
# Y-axis: Crime Type
# Color: Arrest (True/False)

# This is a safer and often better visualization for this data anyway.
# I will modify `analysis.py` to return the counts of True/False for the top types/locations.
# And modify `streamlit_app.py` to plot Stacked Bars.

# But I should double check the column name case sensitivity.
# The user said "you can check the table header".
# I'll try to read one row using `python` and `pandas` if I can get the connection string.
# I will accept that I can't easily run the script without credentials.
# I'll assume the column is `arrest` (lowercase) based on previous working code in `analysis.py`.

# Let's look at `get_top_crime_types` in `analysis.py` again.
# I was doing:
# SELECT primary_type, COUNT(*), SUM(...)
# ... GROUP BY primary_type

# If I want stacked bars, I need to group by `primary_type` AND `arrest`.
# But first I need to find the Top 10 types by TOTAL count.
# So it's a two-step process or a window function.
# 1. Get Top 10 types.
# 2. Get counts for those 10 types, grouped by arrest.

# Plan:
# 1. Modify `get_top_crime_types` to return data suitable for stacked bars.
#    Actually, keeping it simple:
#    - Fetch Top 10 types first.
#    - Then fetch the breakdown for these types.
#    OR just fetch all and filter in Python? No, dataset is large.
#    - Use a subquery or join.

# Refined Plan:
# 1. Modify `analysis.py`:
#    - Rename `get_top_crime_types` to `get_top_crime_types_stacked`.
#    - Query:
#      SELECT primary_type, arrest, COUNT(*) as count 
#      FROM chicago_crimes 
#      WHERE primary_type IN (SELECT primary_type FROM (SELECT primary_type FROM chicago_crimes GROUP BY primary_type ORDER BY COUNT(*) DESC LIMIT 10) as sub)
#      GROUP BY primary_type, arrest
#    - Wait, nested limit in subquery is tricky in some SQL dialects (MySQL doesn't support LIMIT in IN subquery directly without alias nesting).
#    - Easier: Two queries. 
#      Query 1: Get Top 10 Types list.
#      Query 2: Select ... WHERE primary_type IN (...) GROUP BY primary_type, arrest.
#    - Do the same for Locations.

# 2. Modify `streamlit_app.py`:
#    - Use `px.bar(..., color='arrest', barmode='stack')` (or 'group' if preferred, but stacked is better for "total" context).
#    - This respects "Arrest is a label".

# Let's perform this change.

