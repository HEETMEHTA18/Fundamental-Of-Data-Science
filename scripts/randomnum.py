"""Number guessing game with 1-100 range.

The player must guess a random number between 1 and 100.
The program provides hints (too high/too low) and counts attempts.
"""

import random


def number_guessing_game():
    """Run the number guessing game.
    
    Guides the player through guessing a random number with feedback
    on each guess until they guess correctly.
    """
    print("🎯 Welcome to the Number Guessing Game!")
    print("I'm thinking of a number between 1 and 100.")
    print()

    # Random number
    secret_number = random.randint(1, 100)
    attempts = 0

    while True:
        try:
            guess = int(input("Enter your guess: "))
            attempts += 1

            if guess < secret_number:
                print("Too low! 📉")
            elif guess > secret_number:
                print("Too high! 📈")
            else:
                print(f"🎉 Correct! You guessed it in {attempts} attempts.")
                break
        except ValueError:
            print("⚠️ Please enter a valid number.")


if __name__ == "__main__":
    number_guessing_game()
