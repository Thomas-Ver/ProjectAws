import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;
public class AggregatedData implements Serializable {

    public List<List<String>> data = new ArrayList<>();
    public long MeanTfdPerday = 0;
    public long VarianceTfdPerday = 0;
    public long MeanTfpPerday = 0;
    public long VarianceTfpPerday = 0;

    void add(List<String> Value) {
        long Tfd = Long.parseLong(Value.get(1));
        long Tfp = Long.parseLong(Value.get(2));
        data.add(Value);
        NewMeanAndVariance(Tfd, Tfp);
    }

    // We are calculating the mean and the standard deviation with incremental
    // formula
    void NewMeanAndVariance(long NewTfd, long NewTfp) {

        // start the calcul with the old mean
        VarianceTfdPerday = VarianceTfdPerday * (data.size() - 1);
        VarianceTfpPerday = VarianceTfpPerday * (data.size() - 1);
        // we update the mean
        MeanTfdPerday += (NewTfd - MeanTfdPerday) / data.size();
        MeanTfpPerday += (NewTfp - MeanTfpPerday) / data.size();
        // we use the new mean to end-up our calculuse of the new variance
        VarianceTfdPerday += (NewTfd - MeanTfdPerday) * (NewTfd - MeanTfdPerday) / data.size();
        VarianceTfpPerday += (NewTfp - MeanTfpPerday) * (NewTfp - MeanTfpPerday) / data.size();

    }
    @Override
    public String toString() {
        return "AggregatedData{" +
                "data=" + data +
                ", MeanTfdPerday=" + MeanTfdPerday +
                ", VarianceTfdPerday=" + VarianceTfdPerday +
                ", MeanTfpPerday=" + MeanTfpPerday +
                ", VarianceTfpPerday=" + VarianceTfpPerday +
                '}';
    }

    
}
