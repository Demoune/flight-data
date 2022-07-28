package my.exercise.utils

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}

object TestUtils {
  def remove(root: java.nio.file.Path, deleteRoot: Boolean = true): Unit = {
    Files.walkFileTree(root, new SimpleFileVisitor[java.nio.file.Path] {
      override def visitFile(file: java.nio.file.Path, attributes: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        if (deleteRoot) Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    })
  }

}
