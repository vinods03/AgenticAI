Looks for DQ rules in Config.py for multiple sources
pipeline_monitor.py looks for the sources in the specified path/paths of Config.py.
pipeline_monitor.py also csv_handler to read the source and data_quality to run the defined DQ checks / return the results.

IMPORTANT Note: 
For just data quality monitoring, a simple Python script is easiest. 
MCP only makes sense if you're building this as part of a larger Claude Desktop integration where you want conversational access to DQ checks.

The MCP Sweet Spot:
"Can a human analyst benefit from conversing with their data tools live?"
If yes → MCP
If no → Python script/framework

Perfect MCP Projects:
1. Interactive SQL Query Assistant
Why MCP wins:

User asks questions in natural language → Claude generates SQL → executes on live DB → returns results
Iterative exploration: "show me sales", "break down by region", "compare to last month"
MCP keeps DB connection alive across conversation
Better than: OpenAI SDK (no persistent context), Python script (not conversational)

2. Live ETL Pipeline Monitor & Fixer
Why MCP wins:

Monitor Airflow/pipeline status in real-time
Claude can restart failed jobs, check logs, diagnose issues conversationally
"Why did my pipeline fail?" → Claude checks logs + suggests fixes + can apply them
Better than: Scripts (reactive only), CrewAI (too complex for real-time interaction)

3. Data Catalog Explorer
Why MCP wins:

"Find tables with customer email" → searches metadata
"Show me schema of users table" → retrieves instantly
"What's the lineage of revenue metric?" → traces through systems
Keeps metadata connections alive throughout conversation

4. Real-time Analytics Assistant
Why MCP wins:

Connected to Redis/ClickHouse/time-series DB
"What's traffic right now?", "Any anomalies in last hour?"
Creates visualizations on-the-fly as artifacts
Better than: Dashboards (limited queries), Scripts (not interactive)


