Given ETL requirements, the CrewAI agents design and develop the pipeline, using Pyspark.
Test scripts are generated.
Documentation is also generated.

General steps followed (Cursor Terminal):

uv tool install crewai
From C:\Users\VINOD\projects\agents> cd 3_crew
crewai create crew automated_etl_pipeline - >Choose openai, gpt-4o-mini

A folder structure will be created:
Under src/engineering_team/config, modify agents.yaml & tasks.yaml based on your requirement. 
Then, modify crew.py to point to above agents / tasks and main.py to pass the required inputs.

Finally execute in cursor terminal:
crewai run

Inspect the output folder and make changes in agent/task instructions, if required.
