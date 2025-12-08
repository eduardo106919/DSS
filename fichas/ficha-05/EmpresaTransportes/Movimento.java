
import java.time.LocalDate;

public class Movimento {

    private int numero;
    private LocalDate horaEntrada;
    private String localEntrada;
    private Bilhete bil;

    public LocalDate getHora() {
        return this.horaEntrada;
    }

    public String getLocalEntrada() {
        return this.localEntrada;
    }

    public Bilhete getBil() {
        return this.bil;
    }

}