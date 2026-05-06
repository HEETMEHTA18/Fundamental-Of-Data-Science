"""Display random jokes using the pyjokes library."""

import pyjokes


def main():
    """Fetch and display two random jokes."""
    joke = pyjokes.get_joke()
    print(joke)
    joke1 = pyjokes.get_joke()
    print(joke1)


if __name__ == "__main__":
    main()