Certainly! I'll explain how to run the full migration and how to perform a performance test using this script. Let's break it down into two scenarios.



Scenario 1: Running the Full Migration

To run the full migration with this script, follow these steps:

1. First, ensure you've replaced all placeholder values in the script with your actual configuration:
   - `YOUR_DB_SERVER`
   - `YOUR_DB_NAME`
   - `YOUR_DB_USERNAME`
   - `YOUR_DB_PASSWORD`
   - `your_data_root`
   - `YOUR_INSTANCE`
   - `YOUR_USERNAME`

2. Open a terminal or command prompt.

3. Navigate to the directory containing the script.

4. Run the script with your desired parameters. Here's an example command:

   ```
   python data_retrieval_script.py --threads 8 --batch-size 1000 --min-disk-space 15
   ```

   This command will:
   - Use 8 threads for parallel processing
   - Process documents in batches of 1000
   - Stop creating new threads if the free disk space falls below 15%

5. The script will connect to the database, retrieve document information, download the documents, and organize them into the specified directory structure.

6. Monitor the console output for progress information and any warnings or errors.

7. Check the `error_log.json` file for detailed information about any failed document retrievals.

Scenario 2: Running a Performance Test

To run only a performance test without performing the full migration, follow these steps:

1. Ensure you've replaced all placeholder values as described in step 1 of the full migration scenario.

2. Open a terminal or command prompt.

3. Navigate to the directory containing the script.

4. Run the script with the `--performance-test` flag. Here's an example command:

   ```
   python data_retrieval_script.py --performance-test --batch-size 1000 --min-disk-space 15
   ```

   This command will:
   - Run performance tests with thread counts of 2, 4, 8, 12, and 16 (as defined in `PERFORMANCE_TEST_SIZES`)
   - Use a batch size of 1000 documents
   - Stop the test if the free disk space falls below 15%

5. The script will run tests for each thread count, attempting to download approximately 10GB of data (as defined by `PERFORMANCE_TEST_DATA_SIZE`) or until the minimum disk space threshold is reached.

6. Monitor the console output for progress information and test results.

7. At the end of the test, you'll see a summary of results for each thread count, including:
   - Duration of the test
   - Download speed in MB/s
   - Number of failed document retrievals

8. The script will clean up downloaded files after each test run to ensure a clean slate for the next test.

Important Notes:

- For both scenarios, ensure you have sufficient permissions to connect to the database and write to the specified data directory.
- The performance test mode does not permanently store the downloaded documents. It's designed to measure download speed and script performance under different thread counts.
- In both cases, pay attention to the console output and the `error_log.json` file for any issues that may arise during execution.
- You can adjust the `--batch-size` and `--min-disk-space` parameters as needed for your specific environment and requirements.

By following these instructions, you can either perform a full migration of your documents or run performance tests to determine the optimal configuration for your environment.
