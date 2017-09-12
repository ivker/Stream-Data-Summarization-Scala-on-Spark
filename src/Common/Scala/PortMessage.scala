package Common.Scala

import java.io.{ PrintWriter }
import java.net.ServerSocket

object SaleSimulation {

  def Send(port: Int, filename: String, Message: String) {

    val listener = new ServerSocket(port)
    val socket = listener.accept()
    new Thread() {
      override def run = {
        println("Got client connected from: " + socket.getInetAddress)
        val out = new PrintWriter(socket.getOutputStream(), true)
        println("Send:" + Message)
        out.write(Message + '\n')
        out.flush()
        socket.close()
      }
    }.start()
  }
}  