# Java Compatibility Fix for SparkSession

## Issue
You're encountering `java.lang.UnsupportedOperationException: getSubject is not supported` because:

- **Your Java Version**: Java 24 (OpenJDK 24.0.2)
- **Spark Version**: 3.5.2 
- **Issue**: Java 18+ removed support for `javax.security.auth.Subject.getSubject()` which Hadoop/Spark relies on

## Solutions (Choose One)

### Option 1: Downgrade Java (Recommended)
Install Java 11 or Java 17 using SDKMAN:

```bash
# Install SDKMAN
curl -s "https://get.sdkman.io" | bash

# Install Java 17
sdk install java 17.0.12-tem

# Set as default
sdk default java 17.0.12-tem

# Verify
java -version
```

### Option 2: Use Environment Variable Override
If you must keep Java 24, set the JAVA_HOME specifically for Spark:

```bash
# Find Java 17 installation (if you have it)
/usr/libexec/java_home -V

# Set JAVA_HOME in your environment
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home
```

### Option 3: Modified SparkSession (Current Implementation)
The current code attempts to work around this with `HADOOP_USER_NAME` but Java 24 is fundamentally incompatible.

## Verification

After installing Java 17, test the SparkSession:

```python
from pipelines.utils.spark_session import create_spark_session_with_delta
spark = create_spark_session_with_delta()
print("✅ SparkSession with Delta Lake working!")
spark.stop()
```

## Current Status
- ✅ Fixed version compatibility (PySpark 3.5.2 + Delta Spark 3.2.0)
- ✅ Fixed HADOOP_USER_NAME environment variable
- ❌ Java 24 compatibility issue remains - **Java downgrade required**

The fundamental issue is that Apache Spark/Hadoop has not yet been updated to work with Java 18+ security changes.