# CI/CD with Databricks

Continuous Integration and Continuous Deployment (CI/CD) is a process used to deliver software applications to end-users in a faster, efficient, and reliable manner. CI/CD automates the building, testing, and deployment process, which reduces the time between committing the code changes and releasing it to production.

Databricks is a cloud-based data engineering and analytics platform that provides a collaborative environment for building, managing, and deploying data pipelines, machine learning models, and analytics applications. It also provides an API to programmatically interact with the Databricks workspace.

In this repository, we'll discuss how to set up CI/CD pipeline with Databricks using GitHub Actions, Databricks CLI, and Databricks REST API.

## Prerequisites
* A GitHub account.
* A Databricks account with an active workspace.
* Databricks CLI installed on your local machine.
* An Azure Blob storage account to store build artifacts (Optional).
  
## Setup

__Create a Databricks token__

* Go to your Databricks workspace.
* Click on the user icon in the top-right corner and select "User Settings".
* Click on the "Access Tokens" tab.
* Click on the "Generate New Token" button.
* Give a name to the token, select the "Manage" permission, and click on the "Generate" button.
* Copy the generated token.

__Setup github configuration with Databricks__

* Navigate to Github --> Settings --> Developer Settings --> Personal access tokens
* Generate new token (classic) --> Enter password --> Enter note
* Select repo and workflow scope --> Submit --> Copy token
* Login to Databricks --> User Settings --> Git Integration 
* Git provider (Github) --> Git provider username or email (your git credentials) --> Token

__Configure github actions (CICD)__
* Clone the repo- https://github.com/shivampanicker/cicd_with_databricks.git 
* Create 2 branches
    * *develop*: on main
    * *feature/{username}*: on develop
    * Go to the cloned repository--> Settings--> Actions 
    * General --> Allow all Actions and reusable workflows

__Configure secrets__
* Go to Settings--> Secrets and Variables
  * Actions
  * DATABRICKS_HOST_PRD: https://< databricks_workspace_name >.com/
  * DATABRICKS_TOKEN_PRD: < personal access token generated in databricks >

__Action time!__
* Go to Databricks Repos and clone your repository.
* Checkout branch- feature/< username > 
* There are few code level changes required before one raises pull request.
* Update the bronze unit test- test_load_data_into_bronze and set expected_num_files = 2
* Update the silver unit tests and add a new assertion. You are free to write any test case.
* Review the integration_suite test which uses Files in Repos feature and fill in the missing elements.
    * src/main/python/gold/gold_layer_etl.py
    * src/main/tests/integration_suite/test_integration_gold_layer_etl
* Review the dbx deployment file and github workflow files to understand the CICD plan.
* Create a pull request and view the CICD unit testing job that spins up in github → actions.
* Once it succeeds, merge the pull request into develop branch and view the CICD integration testing job that spins up.
* Once integration tests are completed on develop branch, raise a PR from develop branch into main. View the CICD job that spins up, runs unit & integration tests.
* Once it succeeds, merge the pull request into develop branch and view the CICD job that creates Databricks workflow jobs and launches them.
