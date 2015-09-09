package com.netflix.dyno.recipes.util;

public class Tuple<T1, T2>  {

    private final T1 a;
    private final T2 b;

    public Tuple(T1 a, T2 b) {
        this.a = a;
        this.b = b;
    }

    public T1 _1() {
        return a;
    }

    public T2 _2() {
        return b;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((a == null) ? 0 : a.hashCode());
        result = prime * result + ((b == null) ? 0 : b.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Tuple other = (Tuple) obj;
        if (a == null) {
            if (other.a != null) return false;
        } else if (!a.equals(other.a)) return false;
        if (b == null) {
            if (other.b != null) return false;
        } else if (!b.equals(other.b)) return false;
        return true;
    }

    public static <T1, T2> Tuple<T1, T2> as(T1 t1, T2 t2) {
        return new Tuple<T1, T2>(t1, t2);
    }
}
