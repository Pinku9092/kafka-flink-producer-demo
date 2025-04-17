package com.pinku;

import com.pinku.pojos.Employee;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DuplicationFunction  extends KeyedProcessFunction<Integer, Employee, Employee> {
    private transient ValueState<Boolean> seenState;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>(
                "seen", // State name
                Types.BOOLEAN // State type
        );
        seenState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Employee employee, Context context, Collector<Employee> out) throws Exception {
        if (seenState.value() == null) {
            // If not seen, mark as seen and collect the record
            seenState.update(true);
            out.collect(employee);
        }
        // Otherwise, ignore the duplicate
    }
}

