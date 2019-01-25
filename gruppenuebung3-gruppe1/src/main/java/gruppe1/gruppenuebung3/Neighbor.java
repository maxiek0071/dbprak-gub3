package gruppe1.gruppenuebung3;

public class Neighbor {

	private int exclusive;
	private String word;

	public Neighbor(String word, int exclusive) {
		this.exclusive = exclusive;
		this.word = word;
	}

	public int getExclusive() {
		return exclusive;
	}

	public String getWord() {
		return word;
	}

}
