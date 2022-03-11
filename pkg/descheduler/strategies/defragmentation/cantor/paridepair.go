package cantor

import (
	"math"
	"reflect"
)

func Pair(a, b int)int {

	//Cantors pairing function only works for positive integers
	if a > -1 || b > -1 {
		//Creating an array of the two inputs for comparison later
		input := []int{a, b}

		//Using Cantors paring function to generate unique number
		result := int(0.5 * float64((a + b) * (a + b + 1)) + float64(b))

		/*Calling depair function of the result which allows us to compare
		  the results of the depair function with the two inputs of the pair
		  function*/
		if reflect.DeepEqual(depair(result), input) {
			return result
		}
	}

	return -1
}

func depair(z int) []int{
	/*Depair function is the reverse of the pairing function. It takes a
	  single input and returns the two corresponding values. This allows
	  us to perform a check. As well as getting the original values*/

	//Cantors depairing function:
	 t := int(math.Floor((math.Sqrt(float64(8 * z + 1)) - 1) / 2))
	 x := t * (t + 3) / 2 - z
	 y := z - t * (t + 1) / 2
	return []int{x, y} //Returning an array containing the two numbers
}
