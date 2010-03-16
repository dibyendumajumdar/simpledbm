package org.simpledbm.common.impl.info;

import java.util.concurrent.atomic.AtomicReference;

import org.simpledbm.common.api.info.InfoStatistic;

public class InfoStatisticImpl extends StatisticImpl implements InfoStatistic {

    AtomicReference<String> value = new AtomicReference<String>();

    public InfoStatisticImpl(String name) {
        super(name);
    }

    public String get() {
        return value.get();
    }

    public void set(String newValue) {
        this.value.getAndSet(newValue);
        super.setLastUpdated();
    }

    @Override
    public StringBuilder appendTo(StringBuilder sb) {
        super.appendTo(sb);
        return sb.append(value.toString());
    }

}
