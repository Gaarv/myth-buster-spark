package octo;

public interface Talker {

    class Builder {

        private Class<? extends Talker> talkerClass;

        public Builder usingClass(Class<? extends Talker> talkerClass) {
            Builder builder = new Builder();
            builder.talkerClass = talkerClass;
            return builder;
        }

        public Talker build() throws TalkerException {
            try {
                return talkerClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new TalkerException("Unable to instanciate a talker of class " + talkerClass.getName(), e);
            }
        }

    }

    static Builder builder() {
        return new Builder();
    }

    void talk(String message);

}
