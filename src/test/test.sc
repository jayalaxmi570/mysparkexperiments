val reg="[0-9]+".r
val adress="10114 Doglas 102"
val s=reg.findFirstIn(adress)
val s1=reg.findAllIn(adress).toArray
val reg1="[0-9]+".r
val adress1="1234 jayalaxmi 123 tampa"
val res=reg1.findAllIn(adress1).toArray
val res1=reg1.findFirstIn(adress1).getOrElse("no match")
res1.foreach { e =>
   println(s"Found a match: $e")
  }
val str="10114 jayalaxmi"
val reg3="[0-9]".r
val res3=str.replaceAll("[0-9]","s")
val reg4="a".r
val ss="ramchandra"
val resq=ss.replaceAll("a","s")
val pattern="([0-9]+) ([A-Za-z]+)".r
val pattern (count, fruit)="550 grapes"


