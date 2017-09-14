/*
 * Copyright (c) 2016 Razeware LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.raywenderlich.cheesefinder;

import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ProgressBar;
import android.widget.Toast;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class BaseSearchActivity extends AppCompatActivity {

  protected CheeseSearchEngine mCheeseSearchEngine;
  protected EditText mQueryEditText;
  protected Button mSearchButton;
  private CheeseAdapter mAdapter;
  private ProgressBar mProgressBar;

  @Override
  protected void onCreate(@Nullable Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    setContentView(R.layout.activity_cheeses);

    RecyclerView list = (RecyclerView) findViewById(R.id.list);
    list.setLayoutManager(new LinearLayoutManager(this));
    list.setAdapter(mAdapter = new CheeseAdapter());

    mQueryEditText = (EditText) findViewById(R.id.query_edit_text);
    mSearchButton = (Button) findViewById(R.id.search_button);
    mProgressBar = (ProgressBar) findViewById(R.id.progress_bar);

    List<String> cheeses = Arrays.asList(getResources().getStringArray(R.array.cheeses));
    mCheeseSearchEngine = new CheeseSearchEngine(cheeses);
  }

  protected void showProgressBar() {
    mProgressBar.setVisibility(View.VISIBLE);
  }

  protected void hideProgressBar() {
    mProgressBar.setVisibility(View.GONE);
  }

  protected void showResult(List<String> result) {
    if (result.isEmpty()) {
      Toast.makeText(this, R.string.nothing_found, Toast.LENGTH_SHORT).show();
      mAdapter.setCheeses(Collections.<String>emptyList());
    } else {
      mAdapter.setCheeses(result);
    }
  }

}
