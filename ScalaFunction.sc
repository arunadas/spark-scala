// functions

//format def <function name>(parameter name : parameter type ... ): return type = { }
// format def <function name>(parameter name: type...) : return type = { }

def squareIt(x : Int) : Int = {
  x * x
}

println(squareIt(2))

def cubeIt(x : Int): Int = {
  x * x * x
}

println(cubeIt(3))

def transformationInt(x : Int , f : Int => Int) :Int = {
  f(x)
}

transformationInt(2, cubeIt)
val result = transformationInt(3 , cubeIt)
println(result)

transformationInt( 2 , x => x * x * x)
transformationInt( 10 , x => x/2)

transformationInt( 2 , x=> {val y = 2 + x * x; y*y})

// EXERCISE
// use .toUpperCase method.
// Write a function that converts a string to upper-case, and use that function of a few test strings.
// Then, do the same thing using a function literal instead of a separate, named function.

def upperString( x: String) : String = {
  x.toUpperCase
}
var a : String = "aruna"
println(upperString("das"))

def transformString(x : String , f : String => String) : String ={
  f(x)
}

transformString("aruna", upperString)

transformString("aruna", x => x + " das")