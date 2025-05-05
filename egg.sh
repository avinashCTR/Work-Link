egg_path="/Users/avinash/Couture-Scala/obelisk-search/couture-search-pipelines/python/code_artifacts/couture_search-2.0.7-py3.9.egg"
git_pput_path="/Users/avinash/Couture-Scala/Work-Link/eggs/avinash_couture_search-2.0.7-py3.9.egg"

mv $egg_path $git_pput_path

cd /Users/avinash/Couture-Scala/Work-Link

git add .
git commit -m "Updated couture_search egg"
git push origin master