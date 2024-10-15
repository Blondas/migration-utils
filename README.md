# Document Retrieval and Processing System

This project is designed for large-scale document retrieval and processing from a DB2 database using IBM Content Manager. It includes components for command generation, multithreaded execution, and performance testing.

## Key Components

1. **command_generation.py**: Generates 'arsadmin retrieve' commands based on database content.
2. **command_executor.py**: Executes retrieval commands with multithreading support.
3. **performance_test.py**: Tests the performance of the command executor with various thread counts.
4. **logging_config.py**: Configures logging for all scripts.

## Key Properties

- Multithreaded execution for improved performance
- Error handling and retry logic
- State management for resumable operations
- Separate error logging
- Disk space checking to prevent storage overflow
- Performance testing capabilities

## Prerequisites

- Python 3.7+
- IBM DB2 database
- IBM Content Manager
- `arsadmin` command-line tool accessible in the system PATH

## Setup

1. Clone the repository (assuming there is repository):
   ```
   git clone [repository_url]
   cd [repository_name]
   ```

2. Install required Python packages:
   ```
   pip install -r requirements.txt
   ```

3. Set up the DB2 database configuration in `db2_config.py` (not provided in the repository):
   ```python
   DB2_CONFIG = {
       'database': 'your_database_name',
       'hostname': 'your_hostname',
       'port': 'your_port',
       'username': 'your_username',
       'password': 'your_password'
   }
   ```

## Usage

### 1. Command Generation

To generate retrieval commands:

```
python command_generation.py
```

This will:
- Connect to the DB2 database
- Retrieve table and document metadata
- Generate 'arsadmin retrieve' commands
- Save commands to `./out/arsadmin_retrieve.txt`

### 2. Document Retrieval

To execute the generated commands:

```
python command_executor.py [--threads N] [--command_file PATH]
```

Options:
- `--threads N`: Number of threads to use (default is 8)
- `--command_file PATH`: Path to the command file (default is './out/arsadmin_commands.txt')

This will:
- Execute 'arsadmin retrieve' commands using multiple threads
- Handle errors and retries
- Save retrieved documents to `./out/data/`

### 3. Performance Testing

To run performance tests:

```
python performance_test.py
```

This will:
- Run tests with 1, 2, 3, 4, and 5 threads
- Measure time to retrieve 5GB of data for each thread count
- Log results to `./out/logs/performance_test.log`

## Logging

Logs are stored in `./out/logs/`:
- `arsadmin_execution.log`: General execution logs
- `arsadmin_execution_err.log`: Error logs for command execution
- `db2_metadata_retrieval_and_command_generation.log`: Logs for command generation
- `db2_metadata_retrieval_and_command_generation_error.log`: Error logs for command generation
- `performance_test.log`: Performance test results

## Notes

- Ensure sufficient disk space for document retrieval.
- The system uses a state file (`./out/execution_state.json`) for resumable operations.
- Adjust the `min_free_space_percent` in `command_executor_multithreading.py` to prevent disk overflow.

## Troubleshooting

- If you encounter database connection issues, verify the DB2 configuration in `db2_config.py`.
- Ensure `arsadmin` is correctly installed and accessible in your system PATH.
- Check the error logs for detailed information on any failures.

