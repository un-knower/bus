package cn.sibat.taxi

import java.io.ByteArrayOutputStream
import java.util.zip.{ZipEntry, ZipInputStream}

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

/**
  * Created by kong on 2017/7/3.
  */
class ZipFileRecordReader extends RecordReader[Text, BytesWritable] {

  private var fsin: FSDataInputStream = null
  private var zip: ZipInputStream = null
  private var currentKey: Text = null
  private var currentValue: BytesWritable = null
  private var isFinished: Boolean = false

  override def getProgress: Float = if (isFinished) 1 else 0

  override def nextKeyValue(): Boolean = {
    var result = true
    var entry: ZipEntry = null
    try {
      entry = zip.getNextEntry
    } catch {
      case e: Exception => if (!ZipFileInputFormat.apply.isLenient) throw e
    }

    if (entry == null){
      isFinished = true
      return false
    }

    currentKey = new Text(entry.getName)

    val bos = new ByteArrayOutputStream()
    val byte = new Array[Byte](8192)
    var temp = true
    while (temp){
      var bytesRead = 0
      try {
        bytesRead = zip.read(byte,0,8192)
      }catch {
        case e:Exception => {
          if (!ZipFileInputFormat.apply.isLenient)
            throw e
          result = false
        }
      }
      if (bytesRead > 0)
        bos.write(byte,0,bytesRead)
      else
        temp = false
    }

    zip.closeEntry()

    currentValue = new BytesWritable(bos.toByteArray)
    result
  }

  override def getCurrentValue: BytesWritable = currentValue

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val fileSplit = split.asInstanceOf[FileSplit]
    val conf = context.getConfiguration
    val path = fileSplit.getPath
    val fs = path.getFileSystem(conf)
    fsin = fs.open(path)
    zip = new ZipInputStream(fsin)
  }

  override def getCurrentKey: Text = currentKey

  override def close(): Unit = {
    try {
      zip.close()
      fsin.close()
    } catch {
      case e: Exception =>
    }
  }
}
