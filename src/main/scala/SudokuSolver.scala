class UnsolvableException extends Exception("sudoku unsolvable")

class SudokuSolver {
  private type Sudoku = List[List[List[Int]]]

  def solve(original: List[List[Int]]): Option[Sudoku] = {
    val solved = original.zipWithIndex.map { case (row, i) =>
      row.zipWithIndex.map { case (value, j) =>
        value match {
          case 0 => List.range(1, 10)
          case x => List(x)
        }
      }
    }
    val initiallyConstrained = getCellsWithOneConstraint(solved).foldLeft(solved) {
      case (currUpdated, (i, j)) => constrain(currUpdated, i, j)
    }

    solveRecursive(initiallyConstrained, 0, 0)
  }

  private def solveRecursive(sudoku: Sudoku, row: Int, col: Int): Option[Sudoku] = {
    for (possibleValue <- sudoku(row)(col)){
      try{
        val withPossibleValue = withValueAt(sudoku, possibleValue, row, col)
        val constraint = constrain(withPossibleValue, row, col)
        if (isPossible(constraint)){
          if(isSolved(constraint)){
            return Some(constraint)
          }
          val solvedRecursive = getNextCellToSolve(constraint) match {
            case Some((nextRow, nextCol)) => solveRecursive(constraint, nextRow, nextCol)
          }
          solvedRecursive match {
            case Some(recursive) => return Some(recursive)
            case None => /*nothing to do*/
          }
        }
      }catch{
        case e: UnsolvableException => /*nothing to do*/
      }
    }
    None
  }

  private def constrain(sudoku: Sudoku, row: Int, col: Int) = constrainBlocks(constrainRows(constrainCols(sudoku, row, col), row, col), row, col)

  private def constrainBlocks(sudoku: Sudoku, rowI: Int, colI: Int): Sudoku = {
    val constraint = getConstrainingValue(sudoku, rowI, colI)
    val blockI = rowI / 3
    val blockJ = colI / 3

    val updated = sudoku.zipWithIndex.map { case (row, i) =>
      row.zipWithIndex.map { case (values, j) =>
        if(i/3 == blockI && j/3 == blockJ && !(i==rowI && j == colI)){
          values.filter(_ != constraint)
        }else{
          values
        }
      }
    }

    getNewConstraints(sudoku, updated).foldLeft(updated) {
      case (currUpdated, (i, j)) => constrainBlocks(currUpdated, i, j)
    }
  }

  private def constrainRows(sudoku: Sudoku, rowI: Int, colI: Int): Sudoku = {
    val constraint = getConstrainingValue(sudoku, rowI, colI)

    val updated = sudoku.zipWithIndex.map{case (row, i) =>
      row.zipWithIndex.map{case (values, j) =>
        if(i == rowI && j != colI){
          values.filter(_ != constraint)
        }else{
          values
        }
      }
    }

    getNewConstraints(sudoku, updated).foldLeft(updated) {
      case (currUpdated, (i, j)) => constrainRows(currUpdated, i, j)
    }
  }

  private def constrainCols(sudoku: Sudoku, rowI: Int, colI: Int): Sudoku = {
    val constraint = getConstrainingValue(sudoku, rowI, colI)

    val updated = sudoku.zipWithIndex.map{case (row, i) =>
      row.zipWithIndex.map{case (values, j) =>
        if(i != rowI && j == colI){
          values.filter(_ != constraint)
        }else{
          values
        }
      }
    }

    getNewConstraints(sudoku, updated).foldLeft(updated) {
      case (currUpdated, (i, j)) => constrainCols(currUpdated, i, j)
    }
  }

  private def getNextCellToSolve(sudoku: Sudoku): Option[Tuple2[Int,Int]] = {
    for((row, i) <- sudoku.zipWithIndex){
      for ((possibleValues, j) <- row.zipWithIndex) {
        if (possibleValues.length > 1){
          return Some((i,j))
        }
      }
    }
    None
  }

  private def withValueAt(sudoku: Sudoku, value: Int, i1: Int, j1: Int): Sudoku = {
    sudoku.zipWithIndex.map { case (row, i2) =>
      row.zipWithIndex.map { case (values, j2) =>
        if(i1==i2 && j1 == j2) {
          List(value)
        }else{
          values
        }
      }
    }
  }

  private def isPossible(sudoku: Sudoku): Boolean = {
    sudoku.forall(row => row.forall(values => values.nonEmpty))
  }

  private def isSolved(sudoku: Sudoku): Boolean = {
    sudoku.forall(row => row.forall(values => values.length == 1))
  }

  private def getConstrainingValue(sudoku: Sudoku, rowI: Int, colI: Int): Int = {
    if(sudoku(rowI)(colI).length != 1){
      throw new UnsolvableException()
    }
    sudoku(rowI)(colI).head
  }

  private def getNewConstraints(oldSudoku: Sudoku, newSudoku: Sudoku): List[(Int, Int)] = {
    oldSudoku.zip(newSudoku).zipWithIndex.flatMap {case ((oldRow, newRow), i) =>
      oldRow.zip(newRow).zipWithIndex.map{case ((oldValues, newValues), j) =>
        if (oldValues.length > 1 && newValues.length == 1){
          (i,j)
        }else {
          null
        }
      }
    }.filter(_!=null)
  }

  private def getCellsWithOneConstraint(sudoku: Sudoku): List[(Int, Int)] = {
    sudoku.zipWithIndex.flatMap{case(row, i) =>
      row.zipWithIndex.map{case(values, j) => if(values.length == 1){
        (i, j)
      }else{
        null
      }}
    }.filter(_!=null)
  }
}

object Main{
  def main(args: Array[String]): Unit = {
    val solved = new SudokuSolver().solve(
      List(
        List(5, 3, 0, 0, 7, 0, 0, 0, 0),
        List(6, 0, 0, 1, 9, 5, 0, 0, 0),
        List(0, 9, 8, 0, 0, 0, 0, 6, 0),
        List(8, 0, 0, 0, 6, 0, 0, 0, 3),
        List(4, 0, 0, 8, 0, 3, 0, 0, 1),
        List(7, 0, 0, 0, 2, 0, 0, 0, 6),
        List(0, 6, 0, 0, 0, 0, 2, 8, 0),
        List(0, 0, 0, 4, 1, 9, 0, 0, 5),
        List(0, 0, 0, 0, 8, 0, 0, 7, 9)
      )
    )

    solved match {
      case Some(solvedSudoku) => pprint(solvedSudoku)
      case None => print("unable to solve sudoku")
    }
  }

  private def pprint(sudoku: List[List[List[Int]]]): Unit = {
    println()
    sudoku.foreach(row => {
      row.foreach(values =>{
        values.foreach(value => print(value) )
        print(" " * ((9 - values.length) * 2 + 1))
      })
      println()
    })
  }
}
