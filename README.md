# CI/CD with Databricks

Continuous Integration and Continuous Deployment (CI/CD) is a process used to deliver software applications to end-users in a faster, efficient, and reliable manner. CI/CD automates the building, testing, and deployment process, which reduces the time between committing the code changes and releasing it to production.

Databricks is a cloud-based data engineering and analytics platform that provides a collaborative environment for building, managing, and deploying data pipelines, machine learning models, and analytics applications. It also provides an API to programmatically interact with the Databricks workspace.

In this repository, we'll discuss how to set up CI/CD pipeline with Databricks using GitHub Actions, Databricks CLI, and Databricks REST API.

Prerequisites
* A GitHub account.
* A Databricks account with an active workspace.
* Databricks CLI installed on your local machine.
* An Azure Blob storage account to store build artifacts (Optional).
  
Setup

1. Create a Databricks token

To use Databricks REST API, you need to create a personal access token in your Databricks workspace. To create a token, follow these steps:

* Go to your Databricks workspace.
* Click on the user icon in the top-right corner and select "User Settings".
* Click on the "Access Tokens" tab.
* Click on the "Generate New Token" button.
* Give a name to the token, select the "Manage" permission, and click on the "Generate" button.
* Copy the generated token.

2. Setup github configuration

* Navigate to Github --> Settings --> Developer Settings --> Personal access tokens
* Generate new token (classic) --> Enter password --> Enter note
* Select repo and workflow scope --> Submit --> Copy token
* Login to Databricks --> User Settings --> Git Integration 
* Git provider (Github) --> Git provider username or email (your git credentials) --> Token

3. 