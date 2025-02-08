package pgc

import "strings"

// isEmpty checks if the provided string is empty or consists solely of whitespace characters.
//
// The function trims leading and trailing whitespace from the input string `s` using
// strings.TrimSpace. It then evaluates the length of the trimmed string. If the length is
// zero, it indicates that the original string was either empty or contained only whitespace,
// and the function returns true. Otherwise, it returns false.
//
// Parameters:
//   - `s`: A string that needs to be checked for emptiness.
//
// Returns:
//
//	A boolean value:
//	 - true if the string is empty or contains only whitespace characters;
//	 - false if the string contains any non-whitespace characters.
//
// Example:
//
//	result := isEmpty("   ") // result will be true
//	result = isEmpty("Hello") // result will be false
func isEmpty(s string) bool {
	trimmed := strings.TrimSpace(s)
	return len(trimmed) == 0
}

// isNotEmpty checks if the provided string is not empty or does not consist solely of whitespace characters.
//
// This function leverages the IsEmpty function to determine whether the input string `s`
// is empty or contains only whitespace. It returns the negation of the result from IsEmpty.
// If IsEmpty returns true (indicating the string is empty or whitespace), isNotEmpty will return false,
// and vice versa.
//
// Parameters:
//   - `s`: A string that needs to be checked for non-emptiness.
//
// Returns:
//
//		 A boolean value:
//	  - true if the string contains at least one non-whitespace character;
//	  - false if the string is empty or contains only whitespace characters.
//
// Example:
//
//	result := isNotEmpty("Hello") // result will be true
//	result = isNotEmpty("   ") // result will be false
func isNotEmpty(s string) bool {
	return !isEmpty(s)
}
