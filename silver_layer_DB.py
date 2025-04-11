# Databricks notebook source
spark.sql("create database if not exists globalretail_silver")

# COMMAND ----------

spark.sql("show databases").show()