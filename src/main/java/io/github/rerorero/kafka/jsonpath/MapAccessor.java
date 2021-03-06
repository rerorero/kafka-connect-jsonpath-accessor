package io.github.rerorero.kafka.jsonpath;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MapAccessor extends AccessorBase {

    private static final ParserListener.TaskGen<GetTaskState> getTaskGen = new GetTaskGen();
    private static final ParserListener.TaskGen<UpdateTaskState> updateTaskGen = new UpdateTaskGen();

    public static class Getter implements Accessor.Getter<Map<String, Object>> {
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
         * Run the tasks generated from JsonPath and get the value from the given Object.
         *
         * @param m Object from which to get the values
         * @return Map of field paths and values for retrieved values
         */
        public Map<String, Object> run(Map<String, Object> m) {
            final GetTaskState state = new GetTaskState(m);
            runTasks(state, tasks);
            return state.pathMap;
        }
    }

    public static class Updater implements Accessor.Updater<Map<String, Object>> {
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
         * Run the tasks generated from JsonPath and create a new Object with updated value.
         *
         * @param org           Original Object value
         * @param valueToUpdate Map of field paths and updated values
         * @return a new Object instance with the passed valueToUpdate applied.
         */
        public Map<String, Object> run(Map<String, Object> org, Map<String, Object> valueToUpdate) {
            final Map<String, Object> updated = copyMap(org);
            if (valueToUpdate.isEmpty()) {
                return updated;
            }
            final UpdateTaskState state = new SelectableUpdateTaskState(updated, valueToUpdate);
            runTasks(state, tasks);
            return updated;
        }

        /**
         * Run the tasks generated from JsonPath and create a new Object with updated value.
         *
         * @param org           Original Object value
         * @param valueToUpdate value to update
         * @return a new Object instance with the passed valueToUpdate applied.
         */
        public Map<String, Object> run(Map<String, Object> org, Object valueToUpdate) {
            final Map<String, Object> updated = copyMap(org);
            final UpdateTaskState state = new SimpleUpdateTaskState(updated, valueToUpdate);
            runTasks(state, tasks);
            return updated;
        }
    }

    private static Map<String, Object> copyMap(Map<String, Object> org) {
        final Map<String, Object> newMap = new HashMap<>();
        org.forEach((key, value) -> {
            final Object obj = org.get(key);
            if (obj == null) {
                return;
            }
            final Schema.Type inferredType = ConnectSchema.schemaType(obj.getClass());
            switch (inferredType) {
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                case BYTES:
                    newMap.put(key, obj);
                    return;
                case MAP:
                    newMap.put(key, copyMap((Map<String, Object>) obj));
                    return;
                case ARRAY:
                    newMap.put(key, copyArray(key, (List<Object>) obj));
                    return;
                default:
                    throw new JsonPathException(value.getClass() + " is not supported for schemaless record field " + key);
            }
        });
        return newMap;
    }

    private static List<Object> copyArray(String key, List<Object> org) {
        return org.stream().map(o -> {
            final Schema.Type inferredType = ConnectSchema.schemaType(o.getClass());
            switch (inferredType) {
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                case BYTES:
                    return o;
                case MAP:
                    return copyMap((Map<String, Object>) o);
                default:
                    throw new JsonPathException(o.getClass() + " is not supported for the element of array field " + key);
            }
        }).collect(Collectors.toList());
    }

    private static class GetTaskState {
        Map<String, Object> pathMap;

        GetTaskState(Map<String, Object> org) {
            this.pathMap = Collections.singletonMap("$", org);
        }
    }

    private static class ObjectSubUpdateParam {
        final String path;
        final String key;
        final Map<String, Object> parent;

        ObjectSubUpdateParam(String path, String key, Map<String, Object> parent) {
            this.path = path;
            this.key = key;
            this.parent = parent;
        }
    }

    private static Map<String, Object> mapObjectSubscript(Map<String, Object> pathMap, String keyName, Function<ObjectSubUpdateParam, Object> onSubscript) {
        final Map<String, Object> updated = new HashMap<>();

        pathMap.forEach((path, cur) -> {
            final String childPath = pathOfObjectSub(path, keyName);
            if (cur instanceof Map == false) {
                throw new JsonPathException("field '" + childPath + "' is not a Map but " + cur.getClass());
            }
            try {
                final Object child = onSubscript.apply(new ObjectSubUpdateParam(childPath, keyName, (Map<String, Object>) cur));
                if (child != null) {
                    updated.put(childPath, child);
                }
            } catch (DataException e) {
                throw new JsonPathException("An error occurred during processing of Map value '" + childPath + "': " + e.getMessage(), e);
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

        UpdateTaskState(Map<String, Object> org) {
            this.pathMap = Collections.singletonMap("$", org);
        }

        abstract Object getNewValue(String path);
    }

    private static class SelectableUpdateTaskState extends UpdateTaskState {
        private final Map<String, Object> newValue;

        SelectableUpdateTaskState(Map<String, Object> org, Map<String, Object> newValue) {
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

        SimpleUpdateTaskState(Map<String, Object> org, Object newValue) {
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
                        if (child instanceof Map || child instanceof List || child == null) {
                            return child;
                        }
                        // if path is found in newValue, modify the Map in the state
                        // otherwise just get the field and return it.
                        final Object newVal = state.getNewValue(param.path);
                        if (newVal != null) {
                            param.parent.put(param.key, newVal);
                            return newVal;
                        } else {
                            return child;
                        }
                    });
        }

        @Override
        public ParserListener.Task<UpdateTaskState> subscriptArray(int index) {
            return state ->
                    state.pathMap = mapSubscriptArray(state.pathMap, index, param -> {
                        Object child = param.parent.get(param.index);
                        if (child instanceof Map || child instanceof List) {
                            return child;
                        }
                        final Object newVal = state.getNewValue(param.path);
                        if (newVal != null) {
                            param.parent.set(param.index, newVal);
                            return newVal;
                        } else {
                            return child;
                        }
                    });
        }
    }
}
