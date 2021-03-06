from __future__ import division

import pandas as pd

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep

import datetime
import json

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import mean_squared_error,accuracy_score
from sklearn.cross_validation import KFold
from sklearn.linear_model import LinearRegression
from sklearn.cross_validation import StratifiedKFold
from sklearn.feature_extraction import DictVectorizer

from scipy.stats import entropy
from collections import Counter
from src.util.dataloader import hdfs, s3, local

def dt(X):
    return datetime.datetime.fromtimestamp(float(X / 1000))

def to_date(X):
    return X.day()

def tag_entropy(X):
    return entropy(Counter(X).values())

class MRJobPopularityRaw(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    combinations = {
        "time": ["time_step_mean", "time_step_cv"],
        "basic": ["surface", "number_activated_users", "number_activations"],
        "community": ["inffected_communities", "activation_entorpy", "user_usage_entorpy", "usage_dominace",
                      "user_usage_dominance"],
        "exposure": ["user_exposure_mean", "user_exposure_cv",
                     "activateion_exposure_mean", "activateion_exposure_cv"],
        "distance": ["diamiter", "step_distance_mean", "step_distance_cv"],
        "topology": ["degree_mean", "degree_cv",
                     "constraint_mean", "constraint_cv",
                     "pagerank_mean", "pagerank_cv"],
        "semantic": ["tag_entropy"]
    }

    combinations["all"] = combinations["time"] + combinations["basic"] + combinations["community"] + combinations["exposure"] + combinations["distance"] + combinations["topology"] + combinations["semantic"]

    target = ["popularity_class", "user_popularity_class"]

    def configure_options(self):
        super(MRJobPopularityRaw, self).configure_options()
        self.add_passthru_arg('--average', type='int', default=0, help='...')
        self.add_passthru_arg('--cluster', type='int', default=10, help='...')
        self.add_passthru_arg('--folds', type='int', default=10, help='...')
        self.add_passthru_arg('--classifier', type='string', default="logit_regression", help='...')
        self.add_passthru_arg('--day_from', type='int', default=15, help='...')
        self.add_passthru_arg('--day_to', type='int', default=45, help='...')
        self.add_passthru_arg('--filesystem', type=str, default='local', help='Which files system will this be reading from')

    def mapper(self, _, line):

        df = pd.read_json(line["raw"])

        #todo I think this is the wrong way around.
        dfa, dfu = self.generate_tables(df)

        df['time'] = df['time'].apply(dt)
        df = df.set_index(pd.DatetimeIndex(df['time']))

        dfu['time'] = dfu['time'].apply(dt)
        dfu = dfu.set_index(pd.DatetimeIndex(dfu['time']))


        df["user_target"] = df["number_activated_users"].values[-1]
        df["activation_target"] = df["number_activations"].values[-1]

        dfu["user_target"] = dfu["number_activated_users"].values[-1]
        dfu["activation_target"] = dfu["number_activations"].values[-1]


        for k, v in dfu.reset_index().iterrows():
                yield {"observations":k, "type":["popularity_class"]}, {"df": v.to_json(),
                                    "word": line["file"].split("/")[-1]}

        for k, v in dfu.reset_index().iterrows():
                yield {"observations":k, "type":["user_popularity_class"]}, {"df": v.to_json(),
                                    "word": line["file"].split("/")[-1]}


    def classifier(self, X, avr):
        if X >= avr:
            return True
        else:
            return False

    def reducer_logit(self, key, values):

        df = {}
        for v in values:
            df[v["word"]] = json.loads(v["df"])
        df = pd.DataFrame(df).T.fillna(0)

        median_pop = df["activation_target"].median()
        median_user_pop = df["user_target"].median()
        #Todo use this in a map to create a new colum which indicates that
        # it is either above or below the avrage target, for each
        # the two colums would be
        df["popularity_class"] = df["activation_target"].apply(self.classifier, args=(median_pop,))
        df["user_popularity_class"] = df["user_target"].apply(self.classifier, args=(median_user_pop,))


        df_t = df[["popularity_class","user_popularity_class"]]

        if len(df) > 1:
            for t in key["type"]:
                if self.options.folds > len(df[t].values):
                    f = len(df[t].values)
                else:
                    f = self.options.folds

                kf = StratifiedKFold(df_t[t].values, n_folds=f, shuffle=True)
                for train_index, test_index in kf:
                    for k, v in self.combinations.iteritems():

                        vec = DictVectorizer(sparse=False)
                        X = vec.fit_transform(df.drop('popularity_class', 1).drop('user_popularity_class', 1)[v].to_dict('records'))

                        X_train, X_test = X[train_index], X[test_index]
                        Y_train, Y_test = df.ix[train_index, t], df.ix[test_index, t]
                        lm = LogisticRegression()
                        if len(set(Y_train)) > 1 and len(X_train) > 1 and len(X_test) > 1 and len(set(Y_test)) > 1:
                            lm.fit(X_train, Y_train)
                            r = accuracy_score(Y_test, lm.predict(X_test))
                            yield None, {"observation_level": key["observations"], "result": r, "combination":k, "target":t, "conf":lm.coef_.tolist()}

    def reducer_liniar(self, key, values):
        #TODO compute the populaity K-Means class, this will be a cotogory valibal in the linear regression
        df = {}

        for v in values:
            df[v["word"]] = json.loads(v["df"])

        df = pd.DataFrame(df).T.fillna(0)

        if len(df) > 1:

            #Generate the kfolds
            kf = KFold(len(df), n_folds=self.options.folds, shuffle=True)
            for train_index, test_index in kf:


                for t in self.target:
                    for k, v in self.combinations_no_c.iteritems():

                        #Generate the test and train datsets
                        X_train, X_test = df.ix[train_index, v], df.ix[test_index, v]
                        Y_train, Y_test = df.ix[train_index, t], df.ix[test_index, t]

                        lm = LinearRegression(normalize=True)
                        lm.fit(X_train, Y_train)
                        r = mean_squared_error(Y_test, lm.predict(X_test))
                        # yield None, {"observation_level": key["observations"], "result": r, "combination":k, "target":t, "target_level": key["target"],"clusters":cnum, "cluster_num":int(num), "popmessure":popk, "conf":lm.coef_.tolist()}



    def generate_tables(self, df):
        result_user = df.drop_duplicates(subset='number_activated_users', keep='first').set_index(
            ['number_activated_users'], verify_integrity=True, drop=False).sort_index()

        result_user["surface_mean"] = result_user["surface"].expanding(min_periods=1).mean()
        result_user["surface_cv"] = result_user["surface"].expanding(min_periods=1).std()
        result_user["surface_var"] = result_user["surface"].expanding(min_periods=1).var()

        result_user["degree_mean"] = result_user["degree"].expanding(min_periods=1).mean()
        result_user["degree_median"] = result_user["degree"].expanding(min_periods=1).median()
        result_user["degree_cv"] = result_user["degree"].expanding(min_periods=1).std()
        result_user["degree_var"] = result_user["degree"].expanding(min_periods=1).var()
        result_user["degree_max"] = result_user["degree"].expanding(min_periods=1).max()
        result_user["degree_min"] = result_user["degree"].expanding(min_periods=1).min()

        result_user["step_distance_mean"] = result_user["step_distance"].expanding(min_periods=1).mean()
        result_user["step_distance_median"] = result_user["step_distance"].expanding(min_periods=1).median()
        result_user["step_distance_cv"] = result_user["step_distance"].expanding(min_periods=1).std()
        result_user["step_distance_var"] = result_user["step_distance"].expanding(min_periods=1).var()
        result_user["step_distance_max"] = result_user["step_distance"].expanding(min_periods=1).max()
        result_user["step_distance_min"] = result_user["step_distance"].expanding(min_periods=1).min()

        result_user["user_exposure_mean"] = result_user["user_exposure"].expanding(min_periods=1).mean()
        result_user["user_exposure_cv"] = result_user["user_exposure"].expanding(min_periods=1).std()
        result_user["user_exposure_var"] = result_user["user_exposure"].expanding(min_periods=1).var()
        result_user["user_exposure_median"] = result_user["user_exposure"].expanding(min_periods=1).median()
        result_user["user_exposure_max"] = result_user["user_exposure"].expanding(min_periods=1).max()
        result_user["user_exposure_min"] = result_user["user_exposure"].expanding(min_periods=1).min()

        result_user["activateion_exposure_mean"] = result_user["activateion_exposure"].expanding(
            min_periods=1).mean()
        result_user["activateion_exposure_cv"] = result_user["activateion_exposure"].expanding(
            min_periods=1).std()
        result_user["activateion_exposure_var"] = result_user["activateion_exposure"].expanding(
            min_periods=1).var()
        result_user["activateion_exposure_median"] = result_user["activateion_exposure"].expanding(
            min_periods=1).median()
        result_user["activateion_exposure_max"] = result_user["activateion_exposure"].expanding(
            min_periods=1).max()
        result_user["activateion_exposure_min"] = result_user["activateion_exposure"].expanding(
            min_periods=1).min()

        result_user["pagerank_mean"] = result_user["pagerank"].expanding(min_periods=1).mean()
        result_user["pagerank_cv"] = result_user["pagerank"].expanding(min_periods=1).std()
        result_user["pagerank_var"] = result_user["pagerank"].expanding(min_periods=1).var()
        result_user["pagerank_median"] = result_user["pagerank"].expanding(min_periods=1).median()
        result_user["pagerank_max"] = result_user["pagerank"].expanding(min_periods=1).max()
        result_user["pagerank_min"] = result_user["pagerank"].expanding(min_periods=1).min()

        result_user["constraint_mean"] = result_user["constraint"].expanding(min_periods=1).mean()
        result_user["constraint_cv"] = result_user["constraint"].expanding(min_periods=1).std()
        result_user["constraint_var"] = result_user["constraint"].expanding(min_periods=1).var()
        result_user["constraint_median"] = result_user["constraint"].expanding(min_periods=1).median()
        result_user["constraint_max"] = result_user["constraint"].expanding(min_periods=1).max()
        result_user["constraint_min"] = result_user["constraint"].expanding(min_periods=1).min()

        v = []
        for i in range(0, len(result_user["tag"])):
            v.append(tag_entro(result_user["tag"].values[0:i+1]))
        result_user["tag_entropy"] = pd.Series(v)

        result_user["time_step"] = result_user["time"].diff()
        result_user["time_step_mean"] = (result_user["time_step"]).expanding(
            min_periods=1).mean()
        result_user["time_step_cv"] = (result_user["time_step"]).expanding(
            min_periods=1).std()
        result_user["time_step_median"] = (result_user["time_step"]).expanding(
            min_periods=1).median()
        result_user["time_step_min"] = (result_user["time_step"]).expanding(
            min_periods=1).min()
        result_user["time_step_max"] = (result_user["time_step"]).expanding(
            min_periods=1).max()
        result_user["time_step_var"] = (result_user["time_step"]).expanding(
            min_periods=1).var()


        #index on the number of activations
        result_act = df.drop_duplicates(subset='number_activations', keep='first').set_index(
            ['number_activations'], verify_integrity=True, drop=False).sort_index()

        #Surface setup
        result_act["surface_mean"] = result_act["surface"].expanding(min_periods=1).mean()
        result_act["surface_cv"] = result_act["surface"].expanding(min_periods=1).std()
        result_act["surface_var"] = result_act["surface"].expanding(min_periods=1).var()

        #Degre setup
        result_act["degree_mean"] = result_act["degree"].expanding(min_periods=1).mean()
        result_act["degree_median"] = result_act["degree"].expanding(min_periods=1).median()
        result_act["degree_cv"] = result_act["degree"].expanding(min_periods=1).std()
        result_act["degree_var"] = result_act["degree"].expanding(min_periods=1).var()
        result_act["degree_max"] = result_act["degree"].expanding(min_periods=1).max()
        result_act["degree_min"] = result_act["degree"].expanding(min_periods=1).min()

        result_act["step_distance_mean"] = result_act["step_distance"].expanding(min_periods=1).mean()
        result_act["step_distance_median"] = result_act["step_distance"].expanding(min_periods=1).median()
        result_act["step_distance_cv"] = result_act["step_distance"].expanding(min_periods=1).std()
        result_act["step_distance_var"] = result_act["step_distance"].expanding(min_periods=1).var()
        result_act["step_distance_max"] = result_act["step_distance"].expanding(min_periods=1).max()
        result_act["step_distance_min"] = result_act["step_distance"].expanding(min_periods=1).min()


        #Activation exposure setup
        result_act["activateion_exposure_mean"] = result_act["activateion_exposure"].expanding(
            min_periods=1).mean()
        result_act["activateion_exposure_cv"] = result_act["activateion_exposure"].expanding(
            min_periods=1).std()
        result_act["activateion_exposure_var"] = result_act["activateion_exposure"].expanding(
            min_periods=1).var()
        result_act["activateion_exposure_median"] = result_act["activateion_exposure"].expanding(
            min_periods=1).median()
        result_act["activateion_exposure_max"] = result_act["activateion_exposure"].expanding(
            min_periods=1).max()
        result_act["activateion_exposure_min"] = result_act["activateion_exposure"].expanding(
            min_periods=1).min()

        #User exposure setup
        result_act["user_exposure_mean"] = result_act["user_exposure"].expanding(min_periods=1).mean()
        result_act["user_exposure_cv"] = result_act["user_exposure"].expanding(min_periods=1).std()
        result_act["user_exposure_var"] = result_act["user_exposure"].expanding(min_periods=1).var()
        result_act["user_exposure_median"] = result_act["user_exposure"].expanding(min_periods=1).median()
        result_act["user_exposure_max"] = result_act["user_exposure"].expanding(min_periods=1).max()
        result_act["user_exposure_min"] = result_act["user_exposure"].expanding(min_periods=1).min()

        #Pagerank setup
        result_act["pagerank_mean"] = result_act["pagerank"].expanding(min_periods=1).mean()
        result_act["pagerank_cv"] = result_act["pagerank"].expanding(min_periods=1).std()
        result_act["pagerank_var"] = result_act["pagerank"].expanding(min_periods=1).var()
        result_act["pagerank_median"] = result_act["pagerank"].expanding(min_periods=1).median()
        result_act["pagerank_max"] = result_act["pagerank"].expanding(min_periods=1).max()
        result_act["pagerank_min"] = result_act["pagerank"].expanding(min_periods=1).min()

        #constraint setup
        result_act["constraint_mean"] = result_act["constraint"].expanding(min_periods=1).mean()
        result_act["constraint_cv"] = result_act["constraint"].expanding(min_periods=1).std()
        result_act["constraint_var"] = result_act["constraint"].expanding(min_periods=1).var()
        result_act["constraint_median"] = result_act["constraint"].expanding(min_periods=1).median()
        result_act["constraint_max"] = result_act["constraint"].expanding(min_periods=1).max()
        result_act["constraint_min"] = result_act["constraint"].expanding(min_periods=1).min()

        v = []
        for i in range(0, len(result_act["tag"])):
            v.append(tag_entro(result_act["tag"].values[0:i+1]))
        result_act["tag_entropy"] = pd.Series(v)

        #Time step setup
        result_act["time_step"] = result_act["time"].diff()
        result_act["time_step_mean"] = (result_act["time_step"]).expanding(
            min_periods=1).mean()
        result_act["time_step_cv"] = (result_act["time_step"]).expanding(
            min_periods=1).std()
        result_act["time_step_median"] = (result_act["time_step"]).expanding(
            min_periods=1).median()
        result_act["time_step_min"] = (result_act["time_step"]).expanding(
            min_periods=1).min()
        result_act["time_step_max"] = (result_act["time_step"]).expanding(
            min_periods=1).max()
        result_act["time_step_var"] = (result_act["time_step"]).expanding(
            min_periods=1).var()
        return result_act, result_user

    def steps(self):
        return [MRStep(
            mapper=self.mapper,
            reducer=self.reducer_logit
               )]

        # return [MRStep(
        #     mapper=self.mapper_time,
        #     reducer=self.reducer_logit_time
        # )]

if __name__ == '__main__':
    MRJobPopularityRaw.run()
