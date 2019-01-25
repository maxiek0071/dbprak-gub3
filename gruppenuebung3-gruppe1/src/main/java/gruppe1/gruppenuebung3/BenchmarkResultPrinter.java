package gruppe1.gruppenuebung3;

import java.awt.FlowLayout;
import java.util.List;

import javax.swing.JFrame;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.category.DefaultCategoryDataset;


public class BenchmarkResultPrinter {
	
	public static void printPerformance(List<BenchmarkResult> results) {
		
		JFrame frm = new JFrame();
		frm.setTitle("Performance");

		frm.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		frm.setSize(1600, 700);
		frm.setLayout(new FlowLayout());
		
		
		
		DefaultCategoryDataset performanceData = new DefaultCategoryDataset();
		DefaultCategoryDataset varianceData = new DefaultCategoryDataset();
		String bmName;
		for(BenchmarkResult bm: results) {
			if (bm != null) {
				bmName = bm.getBenchmarkName();
				performanceData.addValue(bm.getMin(), "min", bmName);
				performanceData.addValue(bm.getAvg(), "avg", bmName);
				performanceData.addValue(bm.getMax(), "max", bmName);
				varianceData.addValue(bm.getVariance(), "var", bmName);
			}
			
		}
		
		JFreeChart chart = ChartFactory.createBarChart("Performance Results", "Benchmark", "Milliseconds", performanceData);
		frm.add(new ChartPanel(chart));
		
		chart = ChartFactory.createBarChart("Variance", "Benchmark", "Variance", varianceData);
		frm.add(new ChartPanel(chart));
		
		frm.setVisible(true);
		

	}
}