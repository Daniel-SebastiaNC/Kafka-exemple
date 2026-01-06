package br.com.alura.ecommerce;


import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            var email = req.getParameter("email");
            var orderId = req.getParameter("uuid");
            var amount =  new BigDecimal(req.getParameter("amount"));
            var order = new Order(orderId, amount, email);

            try (var database = new OrdersDatabase()) {
                if (database.saveNewOrder(order)) {
                    var id = new CorrelationId(NewOrderServlet.class.getSimpleName());
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order, id);

                    System.out.println("New order sent successfully!");

                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("New order sent successfully!");
                } else {
                    System.out.println("Old order received");

                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("Old order received");
                }
            }
        } catch (ExecutionException | InterruptedException | SQLException e) {
            throw new ServletException(e);
        }
    }
}
