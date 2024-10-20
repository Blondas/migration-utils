# Document Retrieval and Processing System

This project is designed for large-scale document retrieval and processing from a DB2 database using IBM Content Manager. It includes components for command generation, asynchronous execution, and performance testing.

## Key Components

1. **command_generation.py**: Generates 'arsadmin retrieve' commands based on database content.
2. **arsadmin_executor.py**: Asynchronously executes retrieval commands.
3. **performance_test.py**: Tests the performance of the command executor with various worker counts.
4. **logging_config.py**: Configures logging for all scripts.

## Key Properties

- Asynchronous execution for improved performance
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

1. Clone the repository:
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
- Save commands to `./out/arsadmin_commands.txt`

### 2. Document Retrieval

To execute the generated commands:

```
python arsadmin_executor.py [--max_workers N] [--command_file PATH]
```

Options:
- `--max_workers N`: Maximum number of asynchronous workers (default is 8)
- `--command_file PATH`: Path to the command file (default is './out/arsadmin_commands.txt')

This will:
- Asynchronously execute 'arsadmin retrieve' commands
- Handle errors and retries
- Save retrieved documents to `./out/data/`

### 3. Performance Testing

To run performance tests:

```
python performance_test.py
```

This will:
- Run tests with 1 to 16 workers
- Measure time to retrieve 5GB of data for each worker count
- Log results to `./out/logs/performance_test.log`

## Logging

Logs are stored in `./out/logs/`:
- `command_executor.log`: General execution logs
- `command_executor.error_log`: Error logs for command execution
- `performance_test.log`: Performance test logs
- `performance_test_error.log`: Error logs for performance testing

## Notes

- Ensure sufficient disk space for document retrieval.
- The system uses a state file (`./out/execution_state.json`) for resumable operations.
- Adjust the `min_free_space_percent` in `arsadmin_executor.py` to prevent disk overflow.

## Troubleshooting

- If you encounter database connection issues, verify the DB2 configuration in `db2_config.py`.
- Ensure `arsadmin` is correctly installed and accessible in your system PATH.
- Check the error logs for detailed information on any failures.