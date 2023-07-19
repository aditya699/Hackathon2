
# House Price Prediction Problem

# Wireframe

![Wireframe](https://github.com/aditya699/ZAMATO-DATA-SCIENCE-PROJECT/assets/64576351/7244ca0c-bf1e-4bcf-825e-995bd165d055)


# Project Architecture
This project follows a multi-stage architecture for data engineering, data analysis, data science, and deployment.

# Data Engineering

Raw Data: The raw training data is stored in the train.csv file.

Preprocessing: The data is preprocessed using Python, resulting in the cleaned data file train_clean.csv.

Data Ingestion: The cleaned data is ingested into a Kafka producer.

Data Consumption: The ingested data is consumed by a Kafka 
consumer.

# Data Analysis

Data Source: The Kafka consumer retrieves the consumed data.

Data Analysis: Pyspark is used for data analysis on the consumed data.

Output: The analyzed data is saved as the model_train file.

# Data Science
Model Training: The model_train file is used for model training using Pycaret. Three models (Gradient Boosting, Extra Trees, Random Forest) are trained, and the best model is selected.

Testing: The trained model is used to make predictions on the test dataset, and the output is saved.

# Deployment

Dockerization: The project is containerized using Docker. A Dockerfile is provided to build the Docker image.

Image Deployment: The Docker image can be deployed to any environment that supports Docker, enabling easy deployment and scalability.

# Tech Stack 
> Python 3.8 (Used for Overall Buliding)

> Pyspark (Used for data analysis)

> Kafka   (Used for Ingestion of data)  

> Pycaret (Used for model training)

> Docker  (Used for Deployment)

# Pipeline

As we get a file for testing we can simply keep the file in place of test.csv in raw folder and run main.py. The predictions would get saved in output.csv and the best model among 3 i.e Gradient Boosting , Random Forest or Extra Trees will be choosen and the output would be saved.

# How to run it?

1.Download the file.

2.Create a new environment with python 3.8

3.Run pip install -r requirements.txt

4.Then you can play will the application

(Note -The docker image is created looking to save it tar format , due to size and compute constraints not pushing to dockerhub yet)

# Retraining

1.We can retrain the entire system with just minimal changes in main.py 

# Conventions Followed

1.Modular Design: The project is divided into different modules such as Data Engineering, Data Analysis, and Data Science. Each module has its own set of responsibilities, making the codebase modular and easier to maintain.

2.Separation of Concerns: The project separates the different stages of the data pipeline, including data ingestion, data preprocessing, data analysis, and model training. This separation allows for better clarity and reduces the complexity of each individual component.

3.Use of Industry-Standard Tools: The project leverages popular tools and technologies such as Kafka, PySpark, and PyCaret. These tools are widely adopted in the industry and provide efficient and reliable solutions for data processing, analysis, and machine learning tasks.

4.Documentation: The README file and diagram provide clear explanations of the project architecture, data flow, and tech stack used. This documentation helps new team members understand the project structure and facilitates collaboration.

5.Dockerization: The project incorporates Docker to containerize the application, making it easier to deploy and run consistently across different environments. Dockerization simplifies the setup process and reduces compatibility issues.

6.Version Control: Although not explicitly mentioned, it is recommended to use version control, such as Git, to manage the source code and track changes. Version control enables collaboration, allows for easy rollback, and provides a history of the project's development.

## 🚀 About Me
A motivated Data Scientist with a proven track record of success in leveraging data analytics and business insights to drive strategic decisions. Experienced in developing and implementing data-driven strategies that maximize ROI. Skilled in using advanced analytics to identify opportunities for growth and efficiency. Currently working as a Business Analyst at NeenOpal, formerly a Data Scientist at Ineuron.ai & Analytics Vidhya, and an alumnus of IIIT Hyderabad & Delhi University.

