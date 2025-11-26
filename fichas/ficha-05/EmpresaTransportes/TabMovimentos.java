import java.util.List;
import java.util.stream.Collectors;

public class TabMovimentos {

    private List<Movimento> movs;

    public void add(Movimento m) {
        if (m != null)
            this.movs.add(m.clone());
    }

    public List<Movimento> getMovs() {
        return movs.stream().map(Movimento::clone).collect(Collectors.toList());
    }

}
