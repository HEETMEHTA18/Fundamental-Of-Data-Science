"""Scheduled computer shutdown utility.

Allows users to schedule a shutdown after a specified time interval.
Includes test mode for safe testing without actual shutdown.
"""

import os
import time


def main():
    """Manage scheduled shutdown process."""
    print("Welcome to the Turnoff Program")
    print("This program will shut down your computer after a specified time.")
    print()

    try:
        time_to_wait = int(input("Enter the time in seconds before shutdown: "))
        if time_to_wait <= 0:
            print("Please enter a positive number.")
            return

        print(f"Your computer will shut down in {time_to_wait} seconds.")
        print()

        confirm = input("Are you sure you want to proceed? (yes/no/test): "
                        ).lower()

        if confirm == "yes":
            print("Shutdown scheduled. Waiting...")
            time.sleep(time_to_wait)
            os.system("shutdown /s /t 1")
        elif confirm == "test":
            print("[TEST MODE] No shutdown will occur. Simulating wait...")
            time.sleep(time_to_wait)
            print("This is where the shutdown would happen.")
        else:
            print("Shutdown cancelled by user.")

    except ValueError:
        print("Invalid input! Please enter a valid number.")
    except KeyboardInterrupt:
        print("\nProgram interrupted by user.")


if __name__ == "__main__":
    main()
