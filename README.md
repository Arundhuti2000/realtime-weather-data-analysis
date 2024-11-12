# Real-time AWS Weather Analysis Project

A scalable system for collecting, processing, and analyzing weather data from multiple U.S. regions using AWS services. This project utilizes AWS Lambda, S3, and CloudWatch to gather weather information from the National Weather Service API.

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Features](#features)
- [AWS Services Used](#aws-services-used)
- [Prerequisites](#prerequisites)
- [Setup Guide](#setup-guide)
- [Contributing](#contributing)
- [Team](#team)

## 🎯 Project Overview

This project collects real-time weather data from 14 strategically selected locations across the United States, covering diverse climate zones from Alaska to Hawaii. The system processes and stores both raw and analyzed weather data, making it suitable for various applications including agriculture, transportation, and emergency response.

## ✨ Features

- Real-time weather data collection from multiple regions
- Comprehensive weather metrics including:
  - Temperature (Celsius & Fahrenheit)
  - Wind speed
  - Barometric pressure
  - Humidity
  - Weather alerts
  - Forecasts
- Automated data collection and processing
- Organized data storage with regional partitioning
- Built-in error handling and logging
- Weather alert from past two days

## 📊 Dashboard Visualization

![Weather Dashboard](/screenshots/Dashboard.png)
_Real-time weather dashboard_

## 🛠 AWS Services Used

- **AWS Lambda**: For scheduled data collection and processing
- **Amazon S3**: For data storage and organization
- **CloudWatch**: For monitoring and scheduling

## 📝 Prerequisites

1. AWS Account
2. Basic understanding of Python
3. Email address for the NWS API user agent

## 🚀 Setup Guide (UI-Based)

### 1. S3 Bucket Creation

1. Open AWS Management Console
2. Navigate to S3
3. Click "Create bucket"
4. Enter bucket name: `weather-data-2024`
5. Keep default region
6. Leave all other settings as default
7. Click "Create bucket"

### 2. Lambda Function Setup

1. Go to AWS Lambda in the console
2. Click "Create function"
3. Select "Author from scratch"
4. Basic settings:
   - Function name: `weather-data-collector`
   - Runtime: Python 3.9
   - Architecture: x86_64
5. Click "Create function"
6. In "Configuration" tab:
   - Set timeout to 5 minutes
   - Memory: 128 MB
7. Copy the Python code from `nws_multiregion_weather_collector.py` into the Lambda editor
8. Click "Deploy"

### 3. IAM Role Setup (Through UI)

1. Go to IAM in AWS Console
2. Click "Roles"
3. Find your Lambda's role
4. Click "Add permissions"
5. Attach these policies:
   ```
   AWSLambdaBasicExecutionRole
   AmazonS3PutObjectPolicy
   ```

### 4. CloudWatch Schedule Setup

1. Go to CloudWatch in AWS Console
2. Click "Rules" under Events
3. Click "Create rule"
4. Select "Schedule"
5. Set rate: 1 hour
6. Add target: Select your Lambda function
7. Click "Create"

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/name`)
3. Commit your changes (`git commit -am 'Add feature'`)
4. Push to the branch (`git push origin feature/name`)
5. Create a Pull Request

## 👥 Team

- Arundhati Das - [Arundhati2000](https://github.com/Arundhuti2000)
- Pranay Ghuge - [Pranay0205](https://github.com/Pranay0205)

## 📜 License

This project is part of the academic curriculum at UMass Dartmouth.

---

**Note:** Make sure to update the Lambda function's environment variables and IAM roles according to your specific AWS account setup.
