import java.util.Date;

public class Movimento {

    private int numero;
    private Date horaEntrada;
    private String localEntrada;
    private Bilhete bil;

    public Date getHora() {
        return horaEntrada;
    }

    public int getNumero() {
        return numero;
    }

    public String getLocalEntrada() {
        return localEntrada;
    }

    public Bilhete getBil() {
        return bil;
    }

}
