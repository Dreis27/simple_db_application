#!/bin/bash

# Compile DBApplication.java
javac DBApplication.java

# Run DBApplication
java -cp ".:sqlite-jdbc-3.45.1.0.jar:slf4j-api-1.7.36.jar" DBApplication