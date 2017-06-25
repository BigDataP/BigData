/**
 * Created by Meritxell Jordana Gavieiro
 * Class created because there're not one on the standard Hadoop Writable classes
 * It represents a Pair of elements
 */
public class Pair<T1, T2> {
    private final T1 first;
    private final T2 second;

    public Pair(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }

    public T1 getFirst() {
        return first;
    }

    public T2 getSecond() {
        return second;
    }

}