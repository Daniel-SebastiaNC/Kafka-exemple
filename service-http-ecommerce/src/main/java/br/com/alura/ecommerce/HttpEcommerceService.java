package br.com.alura.ecommerce;

import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;

public class HttpEcommerceService {
    public static void main(String[] args) throws Exception {
        var server = new Server(8080);

        var context =new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(new ServletHolder(new NewOrderServlet()), "/new");
        context.addServlet(new ServletHolder(new GenerateAllReportServlet()), "/admin/generate-reports");

        server.setHandler(context);

        server.start();
        server.join();
    }
}
