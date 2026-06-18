# Assignment 2: Text Processing and Classification using Apache Spark

```
Contributors:

Hassan Ali
Odedra Mayurbhai Jakharabhai
Petho Dominik
Robea Anda-Teodora
Rusu Paisie
```

---

## 1. Introduction

The purpose of this project is to implement an event-driven serverless application to perform profanity check and sentiment analysis of customer's reviews. The pipeline is built on built on top of MiniStack, a local emulator of AWS cloud services. The goal of the project is to automatically analyze Amazon product reviews at scale: detecting profanity, classifying sentiment, and maintaining a customer ban ledger. Reviews are uploaded to object storage and flow through a chain of Lambda functions, each responsible for one processing stage, with results persisted in a NoSQL database. The implementation covers the full pipeline from data ingestion to final reporting, including idempotency guarantees to handle re-delivered messages safely.