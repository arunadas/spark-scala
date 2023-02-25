import scala.collection.mutable.ListBuffer
// data Structures

//Tuples
//Immutable Lists

val fruits = ("Mango", "Banana", "grapes")
println(fruits)

//refer to elements using one-index
println(fruits._1)
println(fruits._2)
println(fruits._3)

//key value
val sweetFruit = "Fruit" -> "Mango"
println(sweetFruit._2)
println(sweetFruit._1)

//tuple can have mixed datatypes
val aBunchOfStuff = ("onion", 1234, true)
println(aBunchOfStuff)

// Lists
// Like a tuple, but more functionality
// Must be of same type

val vegList = List("Onion", "Potato", "Pumpkin", "Asparagus")

println(vegList)
//zero based
println(vegList(3))
println(vegList(0))

println(vegList.head)
println(vegList.tail)

for (ship <- vegList){ println(ship)}

//map
val backwardShip = vegList.map((ship : String) => {ship.reverse} )
println(backwardShip)

for (ship <- backwardShip){println(ship)}

// reduce() to combine together all the items in a collection using some function
val numberList = List(1,2,3,4,5)
val sum = numberList.reduce( (x: Int, y: Int)=> x+y)
println(sum)

//filters
val withoutfives = numberList.filter((x:Int)=> x !=5)

val withoutthree = numberList.filter(_ != 3)

//concatenate list
val moreNumbers = List(6,7,8)
val lotsOfNumbers = numberList ++ moreNumbers
println(lotsOfNumbers)

val reversed = numberList.reverse
val sorted = reversed.sorted
val lotsOfDuplicate = numberList ++ numberList
val distinctNumberlist = lotsOfDuplicate.distinct
val maxValue = numberList.max
val minValue = numberList.min
val total = numberList.sum
val hasThree = withoutthree.contains(3)

//Maps
val vegMaps = Map("VegLikes" -> "Asparagus", "VegDislike" -> "Onion", "VegOnFence" -> "Potato")
println(vegMaps("VegLikes"))
println(vegMaps.contains("Brinjal"))
val brinjalVeg = util.Try(vegMaps("Brinjal")) getOrElse("UNKNOWN")
println(brinjalVeg)


// EXERCISE
// Create a list of the numbers 1-20; your job is to print out numbers that are evenly divisible by three.
// solve first by iterating through all the items in the list and testing each
// one as you go. Then, do it again by using a filter function.

val twentynumberList  = ListBuffer[Int]()
for (x<-1 to 20){
  twentynumberList += x
}
println(twentynumberList)

for (list <- twentynumberList) {if(list % 3 == 0) println(list)}
val divBy3 = twentynumberList.filter(_ %3==0)
println(divBy3)

val divBy3_2 = twentynumberList.filter((x:Int)=> x%3==0)
println(divBy3_2)

List.range(1,21).filter(_ %3 == 0).foreach(println)