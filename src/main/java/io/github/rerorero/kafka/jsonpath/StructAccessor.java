package io.github.rerorero.kafka.jsonpath;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StructAccessor extends AccessorBase {

    private static final ParserListener.TaskGen<GetTaskState> getTaskGen = new GetTaskGen();
    private static final ParserListener.TaskGen<UpdateTaskState> updateTaskGen = new UpdateTaskGen();

    public static class Getter implements Accessor.Getter<Struct> {
        private final List<ParserListener.Task<GetTaskState>> tasks;

        /**
         * Parse the given JsonPath and build a new Getter instance which is a
         * task runner to retrieve values from the passed Map according to the json path.
         *
         * @param jsonPath JsonPath string
         */
        public Getter(String jsonPath) {
            this.tasks = parse(jsonPath, getTaskGen);
        }

        /**
         * Run the tasks generated from JsonPath and get the value from the given Struct.
         *
         * @param s Struct from which to get the values
         * @return Map of field paths and values for retrieved values
         */
        public Map<String, Object> run(Struct s) {
            final GetTaskState state = new GetTaskState(s);
            runTasks(state, tasks);
            return state.pathMap;
        }
    }

    public static class Updater implements Accessor.Updater<Struct> {
        private final List<ParserListener.Task<UpdateTaskState>> tasks;

        /**
         * Parse the given JsonPath and build a new Updater instance which is a
         * task runner to update the given Map according to the json path.
         *
         * @param jsonPath JsonPath string
         */
        public Updater(String jsonPath) {
            this.tasks = parse(jsonPath, updateTaskGen);
        }

        /**
         * Run the tasks generated from JsonPath and create a new Struct with updated value.
         *
         * @param org           Original Struct value
         * @param valueToUpdate Map of field paths and updated values
         * @return a new Struct instance with the passed valueToUpdate applied.
         */
        public Struct run(Struct org, Map<String, Object> valueToUpdate) {
            final Struct updated = copyStruct(org);
            if (valueToUpdate.isEmpty()) {
                return updated;
            }
            final UpdateTaskState state = new SelectableUpdateTaskState(updated, valueToUpdate);
            runTasks(state, tasks);
            return updated;
        }

        /**
         * Run the tasks generated from JsonPath and create a new Struct with updated value.
         *
         * @param org           Original Struct value
         * @param valueToUpdate value to update
         * @return a new Struct instance with the passed valueToUpdate applied.
         */
        public Struct run(Struct org, Object valueToUpdate) {
            final Struct updated = copyStruct(org);
            final UpdateTaskState state = new SimpleUpdateTaskState(updated, valueToUpdate);
            runTasks(state, tasks);
            return updated;
        }
    }

    private static Struct copyStruct(Struct org) {
        Struct newStruct = new Struct(org.schema());
        for (Field field : org.schema().fields()) {
            final Object obj = org.get(field);
            if (obj == null) {
                continue;
            }

            final Schema fieldSchema = field.schema();
            switch (fieldSchema.type()) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                case BYTES:
                    newStruct.put(field, obj);
                    break;
                case STRUCT:
                    newStruct.put(field, copyStruct((Struct) obj));
                    break;
                case ARRAY:
                    newStruct.put(field, copyArray((List<Object>) obj, field));
                    break;
                default:
                    throw new JsonPathException(fieldSchema.type() + " is not supported for field " + field.name());
            }
        }
        return newStruct;
    }

    private static class GetTaskState {
        Map<String, Object> pathMap;

        GetTaskState(Struct org) {
            this.pathMap = Collections.singletonMap("$", org);
        }
    }

    private static List<Object> copyArray(List<Object> org, Field field) {
        Schema valueSchema = field.schema().valueSchema();
        return org.stream().map(o -> {
            switch (valueSchema.type()) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                case BYTES:
                    return o;
                case STRUCT:
                    return copyStruct((Struct) o);
                default:
                    throw new JsonPathException(valueSchema.type() + " is not supported for field " + field.name());

            }
        }).collect(Collectors.toList());
    }

    private static class ObjectSubUpdateParam {
        final String path;
        final String key;
        final Struct parent;

        ObjectSubUpdateParam(String path, String key, Struct parent) {
            this.path = path;
            this.key = key;
            this.parent = parent;
        }
    }

    private static Map<String, Object> mapObjectSubscript(Map<String, Object> pathMap, String keyName, Function<ObjectSubUpdateParam, Object> onSubscript) {
        final Map<String, Object> updated = new HashMap<>();

        pathMap.forEach((path, cur) -> {
            final String childPath = pathOfObjectSub(path, keyName);
            if (cur instanceof Struct == false) {
                throw new JsonPathException("field '" + childPath + "' is not a Struct but " + cur.getClass());
            }
            try {
                Struct parent = (Struct) cur;
                // If the specified field is missing, skip it without error. This is the same behavior as Map.
                if (parent.schema().field(keyName) == null) {
                    return;
                }
                final Object child = onSubscript.apply(new ObjectSubUpdateParam(childPath, keyName, parent));
                if (child != null) {
                    updated.put(childPath, child);
                }
            } catch (DataException e) {
                throw new JsonPathException("An error occurred during processing of Struct field '" + childPath + "': " + e.getMessage(), e);
            }
        });

        return updated;
    }

    private static class GetTaskGen implements ParserListener.TaskGen<GetTaskState> {
        @Override
        public ParserListener.Task<GetTaskState> subscriptObject(String keyName) {
            return state ->
                    state.pathMap = mapObjectSubscript(state.pathMap, keyName, param -> param.parent.get(param.key));
        }

        @Override
        public ParserListener.Task<GetTaskState> subscriptArray(int index) {
            return state ->
                    state.pathMap = mapSubscriptArray(state.pathMap, index, param -> param.parent.get(param.index));
        }
    }

    private static abstract class UpdateTaskState {
        Map<String, Object> pathMap;

        UpdateTaskState(Struct org) {
            this.pathMap = Collections.singletonMap("$", org);
        }

        abstract Object getNewValue(String path);
    }

    private static class SelectableUpdateTaskState extends UpdateTaskState {
        private final Map<String, Object> newValue;

        SelectableUpdateTaskState(Struct org, Map<String, Object> newValue) {
            super(org);
            this.newValue = newValue;
        }

        @Override
        Object getNewValue(String path) {
            return newValue.get(path);
        }
    }

    private static class SimpleUpdateTaskState extends UpdateTaskState {
        private final Object newValue;

        SimpleUpdateTaskState(Struct org, Object newValue) {
            super(org);
            this.newValue = newValue;
        }

        @Override
        Object getNewValue(String path) {
            return this.newValue;
        }
    }

    private static class UpdateTaskGen implements ParserListener.TaskGen<UpdateTaskState> {

        @Override
        public ParserListener.Task<UpdateTaskState> subscriptObject(String keyName) {
            return state ->
                    state.pathMap = mapObjectSubscript(state.pathMap, keyName, param -> {
                        Object child = param.parent.get(param.key);
                        if (child instanceof Struct || child instanceof List || child == null) {
                            return child;
                        }
                        // if path is found in newValue, modify the Struct in the state
                        // otherwise just get the field and return it.
                        final Object newVal = state.getNewValue(param.path);
                        if (newVal != null) {
                            param.parent.put(param.key, newVal);
                            return newVal;
                        } else {
                            return param.parent.get(param.key);
                        }
                    });
        }

        @Override
        public ParserListener.Task<UpdateTaskState> subscriptArray(int index) {
            return state ->
                    state.pathMap = mapSubscriptArray(state.pathMap, index, param -> {
                        Object child = param.parent.get(param.index);
                        if (child instanceof Struct || child instanceof List) {
                            return child;
                        }
                        final Object newVal = state.getNewValue(param.path);
                        if (newVal != null) {
                            param.parent.set(param.index, newVal);
                            return newVal;
                        } else {
                            return param.parent.get(param.index);
                        }
                    });
        }
    }
}
