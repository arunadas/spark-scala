//if else
if (11>40) println("not possible") else println("scala broke maths")
if (11<40){
  println("possible")
  println("11 smaller than 40")
}
else{
  println("scala broke maths")
  println("!.")
}

// case condition
val number = 2
number match {
  case 1 => println("one")
  case 2 => println("Two")
  case 3 => println("Three")
  case _ => println("something else")
}
//return squared number
for (x <-1 to  4){
  val squared = x*x
  println(squared)
}

//while example
var x = 15
while (x>=0){
  println(x)
  x -= 1
}

//do while
x = 0
do { println(x); x += 1} while (x<= 9)

//expression
{val x =10 ;  x+ 20}

println({val x = 10 ; x+ 20})

// EXERCISE
// Write some code that prints out the first 10 values of the Fibonacci sequence.

var a =  0
var b = 1
var series = 0
println(a)
print(b)
for (x <- 0 to 9){
  series = a + b
  a = b
  b = series
  println(series)
}