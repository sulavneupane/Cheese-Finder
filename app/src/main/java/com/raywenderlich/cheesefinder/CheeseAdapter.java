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

import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import java.util.List;

public class CheeseAdapter extends RecyclerView.Adapter<CheeseAdapter.CheeseViewHolder> {

  private List<String> mCheeses;

  @Override
  public CheeseViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
    LayoutInflater layoutInflater = LayoutInflater.from(parent.getContext());
    View view = layoutInflater.inflate(android.R.layout.simple_list_item_1, parent, false);
    return new CheeseViewHolder(view);
  }

  @Override
  public void onBindViewHolder(CheeseViewHolder holder, int position) {
    holder.title.setText(mCheeses.get(position));
  }

  @Override
  public int getItemCount() {
    return mCheeses == null ? 0 : mCheeses.size();
  }

  public void setCheeses(List<String> cheeses) {
    mCheeses = cheeses;
    notifyDataSetChanged();
  }

  public static class CheeseViewHolder extends RecyclerView.ViewHolder {
    public final TextView title;

    public CheeseViewHolder(View itemView) {
      super(itemView);
      title = (TextView) itemView.findViewById(android.R.id.text1);
    }
  }
}
