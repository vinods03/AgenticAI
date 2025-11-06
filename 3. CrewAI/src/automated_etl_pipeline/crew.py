from crewai import Agent, Crew, Process, Task
from crewai.project import CrewBase, agent, crew, task
from crewai.agents.agent_builder.base_agent import BaseAgent
from typing import List
# If you want to run a snippet of code before or after the crew starts,
# you can use the @before_kickoff and @after_kickoff decorators
# https://docs.crewai.com/concepts/crews#example-crew-class-with-decorators

@CrewBase
class AutomatedEtlPipeline():
    """AutomatedEtlPipeline crew"""

    agents: List[BaseAgent]
    tasks: List[Task]

    @agent
    def data_architect(self) -> Agent:
        return Agent(
            config=self.agents_config['data_architect'], # type: ignore[index]
            verbose=True
        )

    @agent
    def etl_engineer(self) -> Agent:
        return Agent(
            config=self.agents_config['etl_engineer'], # type: ignore[index]
            verbose=True
        )

    @agent
    def quality_assurance_engineer(self) -> Agent:
        return Agent(
            config=self.agents_config['quality_assurance_engineer'], # type: ignore[index]
            verbose=True
        )
  
    @agent
    def documentation_specialist(self) -> Agent:
        return Agent(
            config=self.agents_config['documentation_specialist'], # type: ignore[index]
            verbose=True
        )

    @task
    def data_architecture_task(self) -> Task:
        return Task(
            config=self.tasks_config['data_architecture_task'], # type: ignore[index]
        )

    @task
    def etl_engineer_task(self) -> Task:
        return Task(
            config=self.tasks_config['etl_engineer_task'] # type: ignore[index]
        )

    @task
    def reportquality_assurance_engineer_tasking_task(self) -> Task:
        return Task(
            config=self.tasks_config['quality_assurance_engineer_task'] # type: ignore[index]
        )

    @task
    def documentation_specialist_task(self) -> Task:
        return Task(
            config=self.tasks_config['documentation_specialist_task'] # type: ignore[index]
        )

    @crew
    def crew(self) -> Crew:
        """Creates the AutomatedEtlPipeline crew"""

        return Crew(
            agents=self.agents, # Automatically created by the @agent decorator
            tasks=self.tasks, # Automatically created by the @task decorator
            process=Process.sequential,
            verbose=True,
            # process=Process.hierarchical, # In case you wanna use that instead https://docs.crewai.com/how-to/Hierarchical/
        )
