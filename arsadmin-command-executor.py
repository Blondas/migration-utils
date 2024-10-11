import subprocess
import os
import logging
import shutil
import re


def execute_arsadmin_commands(command_file, state_file, log_file, min_free_space_percent=10):
    logger = logging.getLogger(__name__)
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)

    def get_free_space_percent():
        total, used, free = shutil.disk_usage("/")
        return (free / total) * 100

    def parse_command(command):
        parts = command.split()
        base_command = " ".join(parts[:parts.index("-d") + 2])
        doc_names = parts[parts.index("-d") + 2:]
        output_dir = parts[parts.index("-d") + 1]
        return base_command, doc_names, output_dir

    def execute_command(command):
        logger.info(f"Executing command: {command}")
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate()
        return process.returncode, stdout, stderr

    def save_state(command_index):
        with open(state_file, 'w') as f:
            f.write(str(command_index))

    def load_state():
        if os.path.exists(state_file):
            with open(state_file, 'r') as f:
                return int(f.read().strip())
        return 0

    def ensure_directory_exists(directory):
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Created directory: {directory}")

    command_index = load_state()

    with open(command_file, 'r') as f:
        commands = f.readlines()

    while command_index < len(commands):
        if get_free_space_percent() < min_free_space_percent:
            logger.warning(f"Free disk space below {min_free_space_percent}%. Stopping execution.")
            break

        command = commands[command_index].strip()
        base_command, doc_names, output_dir = parse_command(command)

        # Ensure the output directory exists
        ensure_directory_exists(output_dir)

        while doc_names:
            full_command = f"{base_command} {' '.join(doc_names)}"
            return_code, stdout, stderr = execute_command(full_command)

            if return_code != 0:
                logger.error(f"Command failed with return code {return_code}")
                logger.error(f"Error output: {stderr}")

                if "ARS1159E Unable to retrieve the object" in stderr:
                    match = re.search(r"Unable to retrieve the object (\S+)", stderr)
                    if match:
                        failing_doc = match.group(1)
                        logger.error(f"Failed to retrieve document: {failing_doc}")
                        logger.error(f"Failing command: {base_command} {failing_doc}")
                        doc_names = doc_names[doc_names.index(failing_doc) + 1:]
                    else:
                        logger.error(
                            "Could not identify failing document. Skipping remaining documents in this command.")
                        break
                elif "ARS1168E Unable to determine Storage Node" in stderr or "ARS1110E The application group" in stderr:
                    logger.error(f"Critical error. Skipping command: {full_command}")
                    break
                else:
                    logger.error(f"Unknown error. Skipping remaining documents in this command.")
                    break
            else:
                logger.info("Command executed successfully")
                break  # All documents processed successfully

        command_index += 1
        save_state(command_index)

    logger.info("Finished executing commands")


# Usage example:
# execute_arsadmin_commands('arsadmin_commands.txt', 'execution_state.txt', 'arsadmin_execution.log')


def main():
    execute_arsadmin_commands(
        './out/arsadmin_commands.txt',
        './out/execution_state.txt',
        './out/arsadmin_execution.log',
        './out/arsadmin_error.log'
    )


if __name__ == "__main__":
    main()

# Usage example:
# execute_arsadmin_commands('arsadmin_commands.txt', 'execution_state.txt', 'arsadmin_execution.log', 'arsadmin_error.log')