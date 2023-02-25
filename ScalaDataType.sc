//values are immutable
val val_hello: String = "Hello"

//variables are mutable
var var_hello : String = val_hello
var_hello =  var_hello + " World!"
println(val_hello)
println(var_hello)

val immutable_var_hello : String = val_hello + " World!"
println(immutable_var_hello)

// Data types
val numberOne : Int = 100
val truth: Boolean = true
val letterA: Char = 'c'
val pi : Double = 3.14159265
val piSinglePrecision: Float = 3.14159265f
val bigNumber: Long = 123456789
val smallNumber: Byte = 127

println("Combining vals:" + numberOne + truth + letterA + pi+bigNumber)
println(f"pi is about $piSinglePrecision%.3f")

println(s"s prefix to use variables like $numberOne $truth $letterA")
println(s"the s prefix is not limited to variables . expression included like : ${1+3}")

//regular expression
val nbrOfStates : String = "Number of states in usa, 50."
val pattern = """.* ([\d]+).*""".r
val pattern(answerString) = nbrOfStates
val answer = answerString.toInt
println(answer)

//Booleans
val isGreater = 1>2
val isLesser = 1<2
val impossible  = isGreater & isLesser
val anotherWay = isGreater && isLesser
val anotherWay2 = isGreater || isLesser

val country : String = "India"
val bestCountry: String = "India"
val isBest : Boolean = country == bestCountry

// EXERCISE
// Write some code that takes the value of pi, doubles it, and then prints it within a string with
// five decimal places of precision to the right.
val doublepi : Double = pi*2
println(f" the double pi is: $doublepi%.5f")
