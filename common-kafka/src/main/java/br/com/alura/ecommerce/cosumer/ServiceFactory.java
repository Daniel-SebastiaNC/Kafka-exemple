package br.com.alura.ecommerce.cosumer;

public interface ServiceFactory<T> {
    ConsumerService<T> create() throws Exception;
}
